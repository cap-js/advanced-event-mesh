# Welcome to @cap-js/advanced-event-mesh

[![REUSE status](https://api.reuse.software/badge/github.com/cap-js/advanced-event-mesh)](https://api.reuse.software/info/github.com/cap-js/advanced-event-mesh)



## About this project

CDS plugin providing integration with SAP Integration Suite, advanced event mesh.



## Table of Contents

- [About this project](#about-this-project)
- [Requirements](#requirements)
- [Setup](#setup)
  - [Setting Up the Broker](#setting-up-the-broker)
  - [Broker Validation](#broker-validation)
  - [Additional Configuration Options](#additional-configuration-options)
- [Support, Feedback, Contributing](#support-feedback-contributing)
- [Code of Conduct](#code-of-conduct)
- [Licensing](#licensing)



## Requirements

See [Getting Started](https://cap.cloud.sap/docs/get-started/in-a-nutshell) on how to jumpstart your development and grow as you go with SAP Cloud Application Programming Model (CAP).
To learn about messaging in CAP, please consult the guide on [Events & Messaging](https://cap.cloud.sap/docs/guides/messaging/).



## Setup

Install the plugin via:

```bash
npm add @cap-js/advanced-event-mesh
```

Then, set the `kind` of your messaging service to `advanced-event-mesh`:

```jsonc
{
  "cds": {
    "requires": {
      "messaging": {
        "kind": "advanced-event-mesh"
      }
    }
  }
}
```


### Setting Up the Broker

The broker itself must be created manually in SAP Integration Suite, advanced event mesh and trust must be established to the respective application in [SAP Cloud Identity Services](https://help.sap.com/docs/cloud-identity-services/cloud-identity-services), both for the Solace broker and the [SEMP API](https://docs.solace.com/Admin/SEMP/Using-SEMP.htm).
For details, please consult SAP Integration Suite, advanced event mesh's documentation at [help.pubsub.em.services.cloud.sap](https://help.pubsub.em.services.cloud.sap/Get-Started/get-started-lp.htm) and [help.sap.com](https://help.sap.com/docs/sap-integration-suite/advanced-event-mesh).

Specifically, you need to configure SAP Integration Suite, advanced event mesh to allow your CAP application to connect to the broker.
For this, follow guide [CAP Plugin for SAP Integration Suite, Advanced Event Mesh](https://help.sap.com/docs/sap-integration-suite/advanced-event-mesh/cap-plugin-for-sap-integration-suite-advanced-event-mesh).

Finally, the broker's credentials must be provided via a [user-provided service instance](https://docs.cloudfoundry.org/devguide/services/user-provided.html) with the name `advanced-event-mesh` and credentials in the following format:

```jsonc
{
  "authentication-service": {
    "tokenendpoint": "https://<ias host>/oauth2/token",
    "clientid": "<client id>",
    "clientsecret": "<client secret>"
  },
  "endpoints": {
    "advanced-event-mesh": {
      "uri": "https://<broker host>:<port>",
      "smf_uri": "wss://<broker host>:<port>"
    }
  },
  "vpn": "<vpn>"
}
```

To troubleshoot connection issues, set log level for component `messaging` to `DEBUG`.
Check [`cds.log()`](https://cap.cloud.sap/docs/node.js/cds-log) for how to maintain log levels.


### Broker Validation

Your app must be bound to an instance of service `SAP Integration Suite, advanced event mesh` with plan `aem-validation-service`.
Please see [Validation of VMR Provisioning](https://help.sap.com/docs/sap-integration-suite/advanced-event-mesh/validation-of-vmr-provisioning) for more information.


### Additional Configuration Options

Additional configuration options for the messaging service (i.e., `cds.requires.messaging`) are:

| Property | Type | Description |
| --- | --- | --- |
| `skipManagement` | `boolean` | If set to `true`, the plugin will not create a queue or subscription. This is useful if you want to manage these resources manually, default: `false` |
| `subaccountId` | `string` | The ID of the subaccount in which SAP Integration Suite, advanced event mesh is subscribed. This is necessary in case the app runs in a different subaccount. |
| `session` | [SessionProperties](https://docs.solace.com/API-Developer-Online-Ref-Documentation/nodejs/solace.SessionProperties.html) | Used for [`createSession`](https://docs.solace.com/API-Developer-Online-Ref-Documentation/nodejs/solace.SolclientFactory.html#createSession) |
| `queue`   | [createMsgVpnQueue](https://docs.solace.com/API-Developer-Online-Ref-Documentation/swagger-ui/software-broker/config/index.html#/msgVpn/createMsgVpnQueue) | The queue object which is created via the [SEMP API](https://docs.solace.com/Admin/SEMP/Using-SEMP.htm) |
| `consumer` | [MessageConsumerProperties](https://docs.solace.com/API-Developer-Online-Ref-Documentation/nodejs/solace.MessageConsumerProperties.html) | Used for [`createMessageConsumer`](https://docs.solace.com/API-Developer-Online-Ref-Documentation/nodejs/solace.Session.html#createMessageConsumer) |
| `clientFactory` | [SolclientFactoryProperties](https://docs.solace.com/API-Developer-Online-Ref-Documentation/nodejs/solace.SolclientFactoryProperties.html#SolclientFactoryProperties) | Used to create the [SolclientFactory](https://docs.solace.com/API-Developer-Online-Ref-Documentation/nodejs/solace.SolclientFactory.html) instance |

The default values can be found in the plugin's [package.json](https://github.com/cap-js/advanced-event-mesh/blob/main/package.json).
As always, the effective configuration for your project can be queried via CLI command [`cds env`](https://cap.cloud.sap/docs/node.js/cds-env).

For more details, please refer to the [messaging section](https://cap.cloud.sap/docs/node.js/messaging) of the CAP Node.js documentation.



## Support, Feedback, Contributing

This project is open to feature requests/suggestions, bug reports etc. via [GitHub issues](https://github.com/cap-js/advanced-event-mesh/issues). Contribution and feedback are encouraged and always welcome. For more information about how to contribute, the project structure, as well as additional contribution information, see our [Contribution Guidelines](CONTRIBUTING.md).



## Security / Disclosure

If you find any bug that may be a security problem, please follow our instructions at [in our security policy](https://github.com/cap-js/advanced-event-mesh/security/policy) on how to report it. Please do not create GitHub issues for security-related doubts or problems.



## Code of Conduct

We as members, contributors, and leaders pledge to make participation in our community a harassment-free experience for everyone. By participating in this project, you agree to abide by its [Code of Conduct](https://github.com/cap-js/.github/blob/main/CODE_OF_CONDUCT.md) at all times.



## Licensing

Copyright 2024 SAP SE or an SAP affiliate company and advanced-event-mesh contributors. Please see our [LICENSE](LICENSE) for copyright and license information. Detailed information including third-party components and their licensing/copyright information is available [via the REUSE tool](https://api.reuse.software/info/github.com/cap-js/advanced-event-mesh).
