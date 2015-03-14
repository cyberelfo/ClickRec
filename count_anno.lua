local matches = redis.call('KEYS', 'ANNO:*')

redis.call('DEL', 'ANNO_COUNTS')

for _,key in ipairs(matches) do
    local val = redis.call('LLEN', key)
	redis.call('HINCRBY', 'ANNO_COUNTS', val, 1)
end