local matches = redis.call('KEYS', 'BIT_DOC:*')

redis.call('DEL', 'DOC_COUNTS')

for _,key in ipairs(matches) do
    local val = redis.call('BITCOUNT', key)
    if val == 0 then
        local docid = string.sub(key, 9, -1)

        local annotations = redis.call('LRANGE', 'ANNOTATIONS:' .. docid, 0, -1)
        for _,a in ipairs(annotations) do
            redis.call('LREM', 'URI_DOCS:' .. a, 0, docid)
        end

        local sections = redis.call('LRANGE', 'SECTIONS:' .. docid, 0, -1)
        for _,s in ipairs(sections) do
            redis.call('LREM', 'SEC_DOCS:' .. s, 0, docid)
        end

        redis.call('DEL', key)
    else
        redis.call('ZADD', 'DOC_COUNTS', tonumber(val), key)
    end
end
