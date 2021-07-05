
export function methodMissing(obj, handler) {
    const proxyObject = new Proxy(obj, {
        get(object, property) {
            if (Reflect.has(object, property)) {
                return Reflect.get(object, property);
            } else {
                return handler.call(object, property);
            }
        }
    });
    return proxyObject;
}
