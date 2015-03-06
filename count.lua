local matches = redis.call('KEYS', 'DOCID:*')

redis.call('DEL', 'DOC_COUNTS')

for _,key in ipairs(matches) do
    local val = redis.call('BITCOUNT', key)
    redis.call('ZADD', 'DOC_COUNTS', tonumber(val), key)
end
