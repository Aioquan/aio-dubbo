/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.cluster.Merger;
import org.apache.dubbo.rpc.cluster.merger.MergerFactory;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ScopeModelUtil;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.rpc.Constants.ASYNC_KEY;
import static org.apache.dubbo.rpc.Constants.MERGER_KEY;

/**
 * @param <T>
 */
@SuppressWarnings("unchecked")
public class MergeableClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger log = LoggerFactory.getLogger(MergeableClusterInvoker.class);

    public MergeableClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        checkInvokers(invokers, invocation);
        String merger = getUrl().getMethodParameter(invocation.getMethodName(), MERGER_KEY);
        // 若果未配置拓展，直接调用首个可用的 Invoker 对象
        if (ConfigUtils.isEmpty(merger)) { // If a method doesn't have a merger, only invoke one Group
            for (final Invoker<T> invoker : invokers) {
                if (invoker.isAvailable()) {
                    try {
                        return invokeWithContext(invoker, invocation);
                    } catch (RpcException e) {
                        if (e.isNoInvokerAvailableAfterFilter()) {
                            log.debug("No available provider for service" + getUrl().getServiceKey() + " on group "
                                + invoker.getUrl().getGroup() + ", will continue to try another group.");
                        } else {
                            throw e;
                        }
                    }
                }
            }
            return invokeWithContext(invokers.iterator().next(), invocation);
        }

        // 通过反射，获得返回类型
        Class<?> returnType;
        try {
            returnType = getInterface().getMethod(invocation.getMethodName(), invocation.getParameterTypes()).getReturnType();
        } catch (NoSuchMethodException e) {
            returnType = null;
        }

        // 提交线程池，并行执行，发起 RPC 调用，并添加到 results 中
        Map<String, Result> results = new HashMap<>();
        for (final Invoker<T> invoker : invokers) {
            RpcInvocation subInvocation = new RpcInvocation(invocation, invoker);
            subInvocation.setAttachment(ASYNC_KEY, "true");
            results.put(invoker.getUrl().getServiceKey(), invokeWithContext(invoker, subInvocation));
        }

        Object result;
        // 阻塞等待执行执行结果，并添加到 resultList 中
        List<Result> resultList = new ArrayList<>(results.size());

        for (Map.Entry<String, Result> entry : results.entrySet()) {
            Result asyncResult = entry.getValue();
            try {
                Result r = asyncResult.get(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
                if (r.hasException()) { // 异常 Result ，打印错误日志，忽略
                    log.error("Invoke " + getGroupDescFromServiceKey(entry.getKey()) +
                        " failed: " + r.getException().getMessage(), r.getException());
                } else { // 正常 Result ，添加到 resultList 中
                    resultList.add(r);
                }
            } catch (Exception e) {
                throw new RpcException("Failed to invoke service " + entry.getKey() + ": " + e.getMessage(), e);
            }
        }

        if (resultList.isEmpty()) {
            return AsyncRpcResult.newDefaultAsyncResult(invocation);
        } else if (resultList.size() == 1) {
            return AsyncRpcResult.newDefaultAsyncResult(resultList.get(0).getValue(), invocation);
        }

        // 返回类型为 void ，返回空的 RpcResult
        if (returnType == void.class) {
            return AsyncRpcResult.newDefaultAsyncResult(invocation);
        }
        // 【第 1 种】基于合并方法
        if (merger.startsWith(".")) {
            // 获得合并方法 Method
            merger = merger.substring(1);
            Method method;
            try {
                method = returnType.getMethod(merger, returnType);
            } catch (NoSuchMethodException | NullPointerException e) {
                throw new RpcException("Can not merge result because missing method [ " + merger + " ] in class [ " +
                    returnType.getName() + " ]");
            }
            // 有 Method ，进行合并
            if (!Modifier.isPublic(method.getModifiers())) {
                method.setAccessible(true);
            }
            result = resultList.remove(0).getValue();
            try {
                // 方法返回类型匹配，合并时，修改 result
                if (method.getReturnType() != void.class
                    && method.getReturnType().isAssignableFrom(result.getClass())) {
                    for (Result r : resultList) {
                        result = method.invoke(result, r.getValue());
                    }
                } else { // 方法返回类型不匹配，合并时，不修改 result
                    for (Result r : resultList) {
                        method.invoke(result, r.getValue());
                    }
                }
            } catch (Exception e) {
                // 无 Method ，抛出 RpcException 异常
                throw new RpcException("Can not merge result: " + e.getMessage(), e);
            }
        } else { // 【第 2 种】基于 Merger
            Merger resultMerger;
            ApplicationModel applicationModel = ScopeModelUtil.getApplicationModel(invocation.getModuleModel().getApplicationModel());
            // 【第 2.1 种】根据返回值类型自动匹配 Merger
            if (ConfigUtils.isDefault(merger)) {
                resultMerger = applicationModel.getBeanFactory().getBean(MergerFactory.class).getMerger(returnType);
            } else { // 【第 2.2 种】指定 Merger
                resultMerger = applicationModel.getExtensionLoader(Merger.class).getExtension(merger);
            }
            // 有 Merger ，进行合并
            if (resultMerger != null) {
                List<Object> rets = new ArrayList<>(resultList.size());
                for (Result r : resultList) {
                    rets.add(r.getValue());
                }
                result = resultMerger.merge(rets.toArray((Object[]) Array.newInstance(returnType, 0)));
            } else { // 无 Merger ，抛出 RpcException 异常
                throw new RpcException("There is no merger to merge result.");
            }
        }
        return AsyncRpcResult.newDefaultAsyncResult(result, invocation);
    }


    @Override
    public Class<T> getInterface() {
        return directory.getInterface();
    }

    @Override
    public boolean isAvailable() {
        return directory.isAvailable();
    }

    @Override
    public void destroy() {
        directory.destroy();
    }

    private String getGroupDescFromServiceKey(String key) {
        int index = key.indexOf("/");
        if (index > 0) {
            return "group [ " + key.substring(0, index) + " ]";
        }
        return key;
    }
}
