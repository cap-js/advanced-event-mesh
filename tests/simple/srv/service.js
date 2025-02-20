const cds = require('@sap/cds')

module.exports = async () => {
  const ext = await cds.connect.to('ExtSrv')
  ext.on('changed', async msg => {
    if (msg.data.mustFail) throw new Error('Failed')
    if (msg.data.mustReject) {
      const e = new Error('Failed')
      e.unrecoverable = true
      throw e
    }
    await INSERT.into('db.Messages').entries({
      event: msg.event,
      data: JSON.stringify(msg.data),
      headers: JSON.stringify(msg.headers)
    })
  })
}
