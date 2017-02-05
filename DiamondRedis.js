const redis = require('redis')
const diamondUtils = require('diamond-core')

const { STORE_RECORD, FETCH_RECORD, success } = diamondUtils.operations

const makeRecordKey = (table, id) => `__${table.name}_${id}`

module.exports = class DiamondRedisCache {
  constructor(){
    this.client = redis.createClient()
    this.set = diamondUtils.promisify(this.client.set.bind(this.client))
    this.get = diamondUtils.promisify(this.client.get.bind(this.client))
  }
  storeRecord({ table, record, id }){
    const recordString = diamondUtils.recordUtils.makeRecordString(table, record)
    const recordKey = makeRecordKey(table, id)
    this.set(recordKey, recordString)
  }
  fetchRecord({ table, id }){
    const recordKey = makeRecordKey(table, id)
    return this.get(recordKey).then(result => {
      return success(success(diamondUtils.recordUtils.parseRecord(result, table.schema)))
    })
  }
  message(message){
    switch(message.operation){
      case STORE_RECORD:
        /* the cache should have a method for storing data */
        this.storeRecord(message.data)
        /* the STORE_RECORD message simply expects an empty success message */
        return Promise.resolve(success())
      case FETCH_RECORD:
        return this.fetchRecord(message.data)
    }
  }
}
