---
layout: doc_page
---

# Enabling security

## TLS

Druid supports Transport Layer Security (TLS) for client traffic to/from the Druid cluster and for internal communications
between Druid services.

Configuring TLS involves two parts:
1. Setting up a keystore with a server certificate used by Druid services
2. Setting up a keystore containing trusted certificates used to validate the server certificate in step 1, so that Druid services can talk to each other with TLS

### General Configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.enablePlaintextPort`|Enable/Disable HTTP connector.|`true`|
|`druid.enableTlsPort`|Enable/Disable HTTPS connector.|`false`|

Although not recommended but both HTTP and HTTPS connectors can be enabled at a time and respective ports are configurable using `druid.plaintextPort`
and `druid.tlsPort` properties on each node. Please see `Configuration` section of individual nodes to check the valid and default values for these ports.

### Jetty Server TLS Configuration

Druid uses Jetty as an embedded web server. To get familiar with TLS/SSL in general and related concepts like Certificates etc.
reading this [Jetty documentation](http://www.eclipse.org/jetty/documentation/9.3.x/configuring-ssl.html) might be helpful.
To get more in depth knowledge of TLS/SSL support in Java in general, please refer to this [guide](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html).
The documentation [here](http://www.eclipse.org/jetty/documentation/9.3.x/configuring-ssl.html#configuring-sslcontextfactory)
can help in understanding TLS/SSL configurations listed below. This [document](http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html) lists all the possible
values for the below mentioned configs among others provided by Java implementation.

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.server.https.keyStorePath`|The file path or URL of the TLS/SSL Key store.|none|yes|
|`druid.server.https.keyStoreType`|The type of the key store.|none|yes|
|`druid.server.https.certAlias`|Alias of TLS/SSL certificate for the connector.|none|yes|
|`druid.server.https.keyStorePassword`|The [Password Provider](../operations/password-provider.html) or String password for the Key Store.|none|yes|

Following table contains non-mandatory advanced configuration options, use caution.

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.server.https.keyManagerFactoryAlgorithm`|Algorithm to use for creating KeyManager, more details [here](https://docs.oracle.com/javase/7/docs/technotes/guides/security/jsse/JSSERefGuide.html#KeyManager).|`javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm()`|no|
|`druid.server.https.keyManagerPassword`|The [Password Provider](../operations/password-provider.html) or String password for the Key Manager.|none|no|
|`druid.server.https.includeCipherSuites`|List of cipher suite names to include. You can either use the exact cipher suite name or a regular expression.|Jetty's default include cipher list|no|
|`druid.server.https.excludeCipherSuites`|List of cipher suite names to exclude. You can either use the exact cipher suite name or a regular expression.|Jetty's default exclude cipher list|no|
|`druid.server.https.includeProtocols`|List of exact protocols names to include.|Jetty's default include protocol list|no|
|`druid.server.https.excludeProtocols`|List of exact protocols names to exclude.|Jetty's default exclude protocol list|no|

### Druid's internal communication over TLS

Whenever possible Druid nodes will use HTTPS to talk to each other. To enable this communication Druid's HttpClient needs to
be configured with a proper [SSLContext](http://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLContext.html) that is able
to validate the Server Certificates, otherwise communication will fail.

Since, there are various ways to configure SSLContext, by default, Druid looks for an instance of SSLContext Guice binding
while creating the HttpClient. This binding can be achieved writing a [Druid extension](../development/extensions.html)
which can provide an instance of SSLContext. Druid comes with a simple extension present [here](../development/extensions-core/simple-client-sslcontext.html)
which should be useful enough for most simple cases, see [this](./including-extensions.html) for how to include extensions.
If this extension does not satisfy the requirements then please follow the extension [implementation](https://github.com/druid-io/druid/tree/master/extensions-core/simple-client-sslcontext)
to create your own extension.

### Upgrading Clients that interact with Overlord or Coordinator
When Druid Coordinator/Overlord have both HTTP and HTTPS enabled and Client sends request to non-leader node, then Client is always redirected to the HTTPS endpoint on leader node.
So, Clients should be first upgraded to be able to handle redirect to HTTPS. Then Druid Overlord/Coordinator should be upgraded and configured to run both HTTP and HTTPS ports. Then Client configuration should be changed to refer to Druid Coordinator/Overlord via the HTTPS endpoint and then HTTP port on Druid Coordinator/Overlord should be disabled.


## <a id="auth"></a> Authentication and Authorization

Druid supports authentication and authorization features through extensions. 

There are three types of configuration objects related to authentication and authorization: the Authenticator, the Authorizer, and the Escalator.

A request first passes through a chain of Authenticators, which determine the identity of the requester. 

If a request is successfully authenticated, it will later pass through an Authorizer, which is responsible for making access control decisions based on the identity of the requester and the resources requested.

An Escalator is responsible for attaching authentication credentials to internal requests made by Druid services to other Druid services.

|Property|Type|Description|Default|Required|
|--------|-----------|--------|--------|--------|
|`druid.auth.authenticatorChain`|JSON List of Strings|List of Authenticator type names|["allowAll"]|no|
|`druid.escalator.type`|String|Type of the Escalator that should be used for internal Druid communications. This Escalator must use an authentication scheme that is supported by an Authenticator in `druid.auth.authenticationChain`.|"noop"|no|
|`druid.auth.authorizers`|JSON List of Strings|List of Authorizer type names |["allowAll"]|no|
|`druid.auth.unsecuredPaths`| List of Strings|List of paths for which security checks will not be performed. All requests to these paths will be allowed.|[]|no|
|`druid.auth.allowUnauthenticatedHttpOptions`|Boolean|If true, skip authentication checks for HTTP OPTIONS requests. This is needed for certain use cases, such as supporting CORS pre-flight requests. Note that disabling authentication checks for OPTIONS requests will allow unauthenticated users to determine what Druid endpoints are valid (by checking if the OPTIONS request returns a 200 instead of 404), so enabling this option may reveal information about server configuration, including information about what extensions are loaded (if those extensions add endpoints).|false|no|

### Enabling Authentication/Authorization

### Authenticator Chain
Authentication decisions are handled by a chain of Authenticator instances. A request will be checked by Authenticators in the sequence defined by the `druid.auth.authenticatorChain`.

Authenticator implementions are provided by extensions.

For example, the following authentication chain definition enables the Kerberos and HTTP Basic authenticators, from the `druid-kerberos` and `druid-basic-security` core extensions, respectively:

```
druid.auth.authenticatorChain=["kerberos", "basic"]
```

A request will pass through all Authenticators in the chain, until one of the Authenticators successfully authenticates the request or sends an HTTP error response. Authenticators later in the chain will be skipped after the first successful authentication or if the request is terminated with an error response.

If no Authenticator in the chain successfully authenticated a request or sent an HTTP error response, an HTTP error response will be sent at the end of the chain.

Druid includes two built-in Authenticators, one of which is used for the default unsecured configuration.

#### AllowAll Authenticator

This built-in Authenticator authenticates all requests, and always directs them to an Authorizer named "allowAll". It is not intended to be used for anything other than the default unsecured configuration.

#### Anonymous Authenticator

This built-in Authenticator authenticates all requests, and directs them to an Authorizer specified in the configuration by the user. It is intended to be used for adding a default level of access so 
the Anonymous Authenticator should be added to the end of the authentication chain. A request that reaches the Anonymous Authenticator at the end of the chain will succeed or fail depending on how the Authorizer linked to the Anonymous Authenticator is configured.

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.auth.authenticator.<authenticatorName>.authorizerName`|Authorizer that requests should be directed to.|N/A|Yes|
|`druid.auth.authenticator.<authenticatorName>.identity`|The identity of the requester.|defaultUser|No|

To use the Anonymous Authenticator, add an authenticator with type `anonymous` to the authenticatorChain.

For example, the following enables the Anonymous Authenticator with the `druid-basic-security` extension:

```
druid.auth.authenticatorChain=["basic", "anonymous"]

druid.auth.authenticator.anonymous.type=anonymous
druid.auth.authenticator.anonymous.identity=defaultUser
druid.auth.authenticator.anonymous.authorizerName=myBasicAuthorizer

# ... usual configs for basic authentication would go here ...
```

### Escalator
The `druid.escalator.type` property determines what authentication scheme should be used for internal Druid cluster communications (such as when a broker node communicates with historical nodes for query processing).

The Escalator chosen for this property must use an authentication scheme that is supported by an Authenticator in `druid.auth.authenticationChain`. Authenticator extension implementors must also provide a corresponding Escalator implementation if they intend to use a particular authentication scheme for internal Druid communications.

#### Noop Escalator

This built-in default Escalator is intended for use only with the default AllowAll Authenticator and Authorizer.

### Authorizers
Authorization decisions are handled by an Authorizer. The `druid.auth.authorizers` property determines what Authorizer implementations will be active.

There are two built-in Authorizers, "default" and "noop". Other implementations are provided by extensions.

For example, the following authorizers definition enables the "basic" implementation from `druid-basic-security`:

```
druid.auth.authorizers=["basic"]
```


Only a single Authorizer will authorize any given request.

Druid includes one built in authorizer:

#### AllowAll Authorizer
The Authorizer with type name "allowAll" accepts all requests.

### Default Unsecured Configuration

When `druid.auth.authenticationChain` is left empty or unspecified, Druid will create an authentication chain with a single AllowAll Authenticator named "allowAll".

When `druid.auth.authorizers` is left empty or unspecified, Druid will create a single AllowAll Authorizer named "allowAll".

The default value of `druid.escalator.type` is "noop" to match the default unsecured Authenticator/Authorizer configurations.

### Authenticator to Authorizer Routing

When an Authenticator successfully authenticates a request, it must attach a AuthenticationResult to the request, containing an information about the identity of the requester, as well as the name of the Authorizer that should authorize the authenticated request.

An Authenticator implementation should provide some means through configuration to allow users to select what Authorizer(s) the Authenticator should route requests to.

### Internal System User

Internal requests between Druid nodes (non-user initiated communications) need to have authentication credentials attached. 

These requests should be run as an "internal system user", an identity that represents the Druid cluster itself, with full access permissions.

The details of how the internal system user is defined is left to extension implementations.

#### Authorizer Internal System User Handling

Authorizers implementations must recognize and authorize an identity for the "internal system user", with full access permissions.

#### Authenticator and Escalator Internal System User Handling

An Authenticator implementation that is intended to support internal Druid communications must recognize credentials for the "internal system user", as provided by a corresponding Escalator implementation.

An Escalator must implement three methods related to the internal system user:

```java
  public HttpClient createEscalatedClient(HttpClient baseClient);

  public org.eclipse.jetty.client.HttpClient createEscalatedJettyClient(org.eclipse.jetty.client.HttpClient baseClient);

  public AuthenticationResult createEscalatedAuthenticationResult();
```

`createEscalatedClient` returns an wrapped HttpClient that attaches the credentials of the "internal system user" to requests.

`createEscalatedJettyClient` is similar to `createEscalatedClient`, except that it operates on a Jetty HttpClient.

`createEscalatedAuthenticationResult` returns an AuthenticationResult containing the identity of the "internal system user".

### Reserved Name Configuration Property

For extension implementers, please note that the following configuration properties are reserved for the names of Authenticators and Authorizers:

```
druid.auth.authenticator.<authenticator-name>.name=<authenticator-name>
druid.auth.authorizer.<authorizer-name>.name=<authorizer-name>

```

These properties provide the authenticator and authorizer names to the implementations as @JsonProperty parameters, potentially useful when multiple authenticators or authorizers of the same type are configured.
