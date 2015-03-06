local sum = 0
local matches = redis.call('KEYS', 'DOCID:*')

redis.call('DEL', "freqs")

for _,key in ipairs(matches) do
    local val = redis.call('BITCOUNT', key)
    if val > tonumber(ARGV[1]) then
    	redis.call('LPUSH', "freqs", key)
    	sum = sum + 1
    end
end

return sum