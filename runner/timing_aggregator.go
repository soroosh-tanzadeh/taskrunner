package runner

import (
	"context"

	"github.com/redis/go-redis/v9"
)

var aggregateScript = redis.NewScript(`
local streamKey = KEYS[1]  -- Stream key
local hashKey = KEYS[2] 
local hashField = KEYS[3] 
local start = ARGV[1]      -- Start ID
local end_ = ARGV[2]       -- End ID
local count = ARGV[3]       -- End ID
local field = ARGV[4]      -- Field to calculate average

local function tonumberOrNil(str)
    if str == nil then
        return nil
    else
        return tonumber(str)
    end
end

local function calculateAverage(numbers)
    local sum = 0
    local count = 0
    for i, number in ipairs(numbers) do
        if type(number) == "number" then
            sum = sum + number
            count = count + 1
        end
    end
    if count > 0 then
        return sum / count
    else
        return nil
    end
end
local entries = redis.call('XRANGE', streamKey, start, end_, 'COUNT', count)

local fieldValues = {}
for i, entry in ipairs(entries) do
    for _, value in ipairs(entry) do
        if type(value) == "table" then
            if value[1] == field then
                local v = tonumberOrNil(value[2])
                table.insert(fieldValues, v)
            end
            end
    end
end

local average = calculateAverage(fieldValues)

if average ~= nil then
    redis.call('HSET', hashKey, hashField, average)
end

return average
`)

func (t *TaskRunner) avgOfStream(destination, destinationField, stream, start, end string, count int, field string) (int64, error) {
	result, err := aggregateScript.Eval(context.Background(), t.redisClient, []string{stream, destination, destinationField}, start, end, count, field).Result()
	if err != nil {
		return 0, err
	}

	return result.(int64), nil
}
