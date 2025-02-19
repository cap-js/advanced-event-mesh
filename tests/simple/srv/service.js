const cds = require('@sap/cds')

module.exports = async () => {
  const messaging = await cds.connect.to('messaging')
  messaging.on('someEvent', () => {})
}
