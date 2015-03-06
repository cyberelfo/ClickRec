local sum = 0
local matches = redis.call('KEYS', 'ANNO:*')

for _,key in ipairs(matches) do
    local val = redis.call('LRANGE', key, 0, 0)
    if val[1] == '0' then
    	redis.call('DEL', key)
    	sum = sum + 1
    end
end

return sum