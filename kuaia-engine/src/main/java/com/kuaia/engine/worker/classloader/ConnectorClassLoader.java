package com.kuaia.engine.worker.classloader;

import java.net.URL;
import java.net.URLClassLoader;

public class ConnectorClassLoader extends URLClassLoader {
    public ConnectorClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }
    
    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            // Check if class is already loaded
            Class<?> c = findLoadedClass(name);
            if (c == null) {
                // Try to load from this ClassLoader (child-first)
                try {
                    c = findClass(name);
                } catch (ClassNotFoundException e) {
                    // If not found, fall back to parent
                    c = super.loadClass(name, resolve);
                }
            }
            if (resolve) resolveClass(c);
            return c;
        }
    }
}
