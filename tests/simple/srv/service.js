const cds = require('@sap/cds')

module.exports = async () => {
  const ext = await cds.connect.to('ExtSrv')
  ext.on('changed', async msg => {
    await INSERT.into('db.Messages').entries({
      event: msg.event,
      data: JSON.stringify(msg.data),
      headers: JSON.stringify(msg.headers)
    })
  })
}
