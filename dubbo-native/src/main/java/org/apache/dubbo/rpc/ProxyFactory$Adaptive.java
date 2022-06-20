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
package org.apache.dubbo.rpc;

import model.ScopeModel;
import model.ScopeModelUtil;

public class ProxyFactory$Adaptive implements ProxyFactory {
    public Object getProxy(Invoker invoker) throws RpcException {
        if (invoker == null) {
            throw new IllegalArgumentException("Invoker argument == null");
        }
        if (invoker.getUrl() == null) {
            throw new IllegalArgumentException("Invoker argument getUrl() == null");
        }
        URL url = invoker.getUrl();
        String extName = url.getParameter("proxy", "javassist");
        if (extName == null) {
            throw new IllegalStateException("Failed to get extension (ProxyFactory) name from url (" + url.toString() + ") use keys([proxy])");
        }
        ScopeModel scopeModel = ScopeModelUtil.getOrDefault(url.getScopeModel(), ProxyFactory.class);
        ProxyFactory extension = (ProxyFactory) scopeModel.getExtensionLoader(ProxyFactory.class).getExtension(extName);
        return extension.getProxy(invoker);
    }

    public Object getProxy(Invoker invoker, boolean arg1) throws RpcException {
        if (invoker == null) {
            throw new IllegalArgumentException("Invoker argument == null");
        }
        if (invoker.getUrl() == null) {
            throw new IllegalArgumentException("Invoker argument getUrl() == null");
        }
        URL url = invoker.getUrl();
        String extName = url.getParameter("proxy", "javassist");
        if (extName == null) {
            throw new IllegalStateException("Failed to get extension (ProxyFactory) name from url (" + url.toString() + ") use keys([proxy])");
        }
        ScopeModel scopeModel = ScopeModelUtil.getOrDefault(url.getScopeModel(), ProxyFactory.class);
        ProxyFactory extension = (ProxyFactory) scopeModel.getExtensionLoader(ProxyFactory.class).getExtension(extName);
        return extension.getProxy(invoker, arg1);
    }

    public Invoker getInvoker(Object invoker, Class arg1, URL arg2) throws RpcException {
        if (arg2 == null) {
            throw new IllegalArgumentException("url == null");
        }
        URL url = arg2;
        String extName = url.getParameter("proxy", "javassist");
        if (extName == null) {
            throw new IllegalStateException("Failed to get extension (ProxyFactory) name from url (" + url.toString() + ") use keys([proxy])");
        }
        ScopeModel scopeModel = ScopeModelUtil.getOrDefault(url.getScopeModel(), ProxyFactory.class);
        ProxyFactory extension = (ProxyFactory) scopeModel.getExtensionLoader(ProxyFactory.class).getExtension(extName);
        return extension.getInvoker(invoker, arg1, arg2);
    }
}
