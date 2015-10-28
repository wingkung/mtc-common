/**
 * Created by wing on 2015/5/11.
 */
var util = require('util'), events = require('events');
var redis = require("redis");
var q = require('q'), _ = require('lodash');
var logger = require('log4js').getLogger('redis');
var KEYS =require('./constant').KEYS;
var listener = require('./listener');
var Request = listener.Request;

var RedisClient = function(conf){
    events.EventEmitter.call(this);
    var self = this;
    this.module = conf.module;
    this.id = String(new Date().getTime());
    this.client = redis.createClient(conf.port, conf.host, conf.options);
    this.client.on('error', function () {
        logger.warn('redis connect error');
    });
    this.client.on('connect', function () {
        logger.info('redis connect');
        self.emit('redis:connect');
    });
    this.subscribeClient = redis.createClient(conf.port, conf.host, conf.options);
    this.subscribeClient.on('error', function () {
        logger.warn('redis subscribe connect error');
    });
    this.subscribeClient.on('connect', function () {
        logger.info('redis subscribe connect');
        self.emit('redis:subscribe:connect');
    });

    this.subscribeClient.on('subscribe', function (channelName) {
        logger.info('subscribe', channelName)
    });
    this.subscribeClient.on('message', function (channelName, action) {
        //logger.debug('消息到达', channelName, action);
        var messages = [];
        try {
            action = JSON.parse(action);
        } catch (e) {
            action = {};
        }
        q().then(function(){
            return self.llen(action.box);
        }).then(function(len){
            var p = q();
            for (var i=0;i<len;i++){
                p = p.then(function () {
                    return self.rpop(action.box).then(function (message) {
                        if (message){
                            messages.push(message);
                        }
                    });
                })
            }
            return p.then(function () {
                //logger.info('接收', JSON.stringify(messages));
                _.forEach(messages, function(message){
                    if (action.type == 'request'){
                        var request = {
                            data: message.data,
                            response: function(result){
                                self.response(message, result);
                            }
                        }
                        self.emit('request_' + message.action, request);
                    }else if (action.type == 'event'){
                        var event = {
                            data: message.data
                        }
                        self.emit('event_' + message.action, event);
                    }else if (action.type == 'response'){
                        self.emit('response', messages);
                    }else{
                        logger.warn('未知类型', message);
                    }
                })

            });
        }).catch(function(e){
            logger.warn(e.stack);
        })
    });

    this.on('redis:subscribe:connect', function () {
        this.subscribe(self.id);
        this.subscribe(self.module);
    });
    this.on('response', function (messages) {
        messages.forEach(function (message) {
            listener.response(message);
        })
    })
};

util.inherits(RedisClient, events.EventEmitter);

module.exports = RedisClient;

RedisClient.prototype.set = function(key, value){
    var self = this;
    return q.Promise(function (resolve, reject) {
        self.client.set(key, value, function (err) {
            if(err){
                reject(err);
            }else{
                logger.debug('设置缓存', key, JSON.stringify(value));
                resolve(true);
            }
        })
    })
};


RedisClient.prototype.get = function (key) {
    var self = this;
    return q.Promise(function (resolve, reject) {
        self.client.get(key, function (err, value) {
            if (err){
                reject(err);
            }else{
                logger.debug('获取缓存', JSON.stringify(value));
                resolve(value);
            }
        })
    })
};

RedisClient.prototype.del = function (key) {
    var self = this;
    return q.Promise(function (resolve, reject) {
        self.client.del(key, function (err) {
            if (err){
                reject(err);
            }else{
                logger.debug('删除缓存', key);
                resolve(true);
            }
        })
    })
};

RedisClient.prototype.lock = function (key) {
    var self = this;
    return q().then(function () {
        return q.Promise(function (resolve, reject) {
            self.client.incr(KEYS.LOCK + ':' + key, function (err, v) {
                if (v === 1){
                    resolve(true);
                }else{
                    reject(new Error("can't lock"));
                }
            });
        })
    }).then(function () {
        return q.Promise(function (resolve, reject) {
            self.client.expire(KEYS.LOCK + ':' + key, 5, function (err) {
                if (err){
                    reject(err);
                }else{
                    resolve(true);
                }
            });
        })
    })
};

RedisClient.prototype.unlock = function (key) {
    return this.del(KEYS.LOCK + ':' + key);
};

RedisClient.prototype.expire = function (key, ttl) {
    return q.Promise(function (resolve, reject) {
        self.client.expire(key, ttl, function (err) {
            if (err){
                reject(err);
            }else{
                resolve(true);
            }
        })
    })
};

RedisClient.prototype.hset = function (key, obj, ttl) {
    var self = this;
    return this.hget(key).then(function (doc) {
        if (!doc){
            doc = {};
        }
        for (var k in obj){
            if (!_.isUndefined(obj[k]) && !_.isFunction(obj[k])){
                doc[k] = obj[k];
            }
        }
        return doc;
    }).then(function (doc) {
        return q.Promise(function (resolve, reject) {
            self.client.HMSET(key, doc, function (err) {
                if(err){
                    reject(err);
                }else{
                    logger.debug('h设置缓存', key, JSON.stringify(doc));
                    resolve(true);
                }
            })
        })
    }).then(function () {
        if (ttl){
            return self.client.expire(key, ttl);
        }
    })

};

RedisClient.prototype.hget = function (key) {
    var self = this;
    return q.Promise(function (resolve, reject) {
        self.client.hgetall(key, function (err, obj) {
            if (err){
                reject(err);
            }else{
                logger.debug('h获取缓存', key, JSON.stringify(obj));
                resolve(obj);
            }
        })
    })
};

RedisClient.prototype.hdel = function (key) {
    var self = this;
    return q.Promise(function (resolve, reject) {
        self.client.hdel(key, function (err) {
            if (err){
                reject(err);
            }else{
                logger.debug('h删除缓存',key);
                resolve(true);
            }
        })
    })
};

RedisClient.prototype.llen = function (key) {
    var self = this;
    return q.Promise(function (resolve, reject) {
        self.client.llen(key, function (err, len) {
            if (err){
                reject(err);
            }else{
                resolve(len);
            }
        })
    })
};

var parseJson = function (str) {
    var obj;
    try {
        obj = JSON.parse(str);
    } catch (e) {
        obj = str;
    }
    return obj;
};

RedisClient.prototype.lpush = function (list, values) {
    var self = this;
    var p = q();
    values = values || [];
    if (!_.isArray(values)){
        values = [values];
    }

    values.forEach(function (value) {
        p = p.then(function () {
            return q.Promise(function (resolve, reject) {
                self.client.lpush(list, JSON.stringify(value), function (err) {
                    if (err){
                        reject(err);
                    }else{
                        //logger.debug('队列压入', list, JSON.stringify(value));
                        resolve(true);
                    }
                })
            });
        })
    });
    return p;
};

RedisClient.prototype.rpop = function (list) {
    var self = this;
    return q.Promise(function (resolve, reject) {
        self.client.rpop(list, function (err, str) {
            var obj = parseJson(str);
            if (err){
                reject(err);
            }else{
                //logger.info('队列弹出',list, JSON.stringify(obj));
                resolve(obj);
            }
        })
    })
};

RedisClient.prototype.lindex = function (list, index) {
    var self = this;
    return q.Promise(function (resolve, reject) {
        self.client.lindex(list, index, function (err, str) {
            var obj = parseJson(str);
            if (err){
                reject(err);
            }else{
                //logger.debug('队列获取', list, JSON.stringify(obj));
                resolve(obj);
            }
        })
    })
};

RedisClient.prototype.lrange = function (list, start, stop) {
    var self = this;
    return q.Promise(function (resolve, reject) {
        self.client.lrange(list, start, stop, function (err, strs) {
            var objs = [];
            strs.forEach(function (str) {
                objs.push(parseJson(str));
            });
            if(err){
                reject(err);
            }else{
                //logger.debug('队列范围', list, JSON.stringify(objs));
                resolve(objs);
            }
        });
    })
};

RedisClient.prototype.publish = function (channel, message) {
    var self = this;
    return q.Promise(function (resolve, reject) {
        self.client.publish(channel, JSON.stringify(message), function (err) {
            if (err){
                reject(err);
            }else{
                //logger.debug('发布', channel, message);
                resolve(true);
            }
        })
    })
};

RedisClient.prototype.subscribe = function (name) {
    this.subscribeClient.subscribe('channel:' + name);
};


RedisClient.prototype.request = function (module, action, data, timeout) {
    var self = this;
    var box = 'request_box:' + module;
    var id = new Date().getTime();

    logger.info('发送请求', id, module, action, data);
    this.lpush(box, {
        id: id,
        clientId: this.id,
        action: action,
        data: data
    }).then(function () {
        return self.publish('channel:' + module, {type: 'request', box: box});
    });

    return q.Promise(function (resovle, reject) {
        var request = new Request(id, timeout);
        request.on('timeout', function () {
            logger.warn('请求超时', id, module, self.id, 'timeout');
            reject(new Error('request timeout'));
        });
        request.on('data', function (data) {
            logger.info('请求返回', id, module, self.id, data);
            resovle(data);
        });
        listener.request(request);
    })
};

RedisClient.prototype.response = function (message, data) {
    var self = this;
    var box = 'response_box:' + message.clientId;
    q().then(function(){
        if (!message){
            throw new Error('返回结果请求源未提供');
        }
        return self.lpush(box, {
            id: message.id,
            data: data
        });
    }).then(function () {
        logger.info('返回结果', message.id, message.action, data)
        return self.publish('channel:' + message.clientId, {type: 'response', box: box});
    }).catch(function(e){
        logger.warn('响应异常', e.stack);
    })
};

RedisClient.prototype.event = function (module, action, data) {
    var self = this;
    var box = 'event_box:' + module;
    var id = new Date().getTime();
    logger.info('发送事件', id, module, action, data);
    return this.lpush(box, {
        id: id,
        action: action,
        data: data
    }).then(function () {
        return self.publish('channel:' + module, {type: 'event', box: box});
    })
};
