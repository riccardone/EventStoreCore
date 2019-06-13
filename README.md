# Event Store (on .net core)  

The open-source, functional database with Complex Event Processing in JavaScript.

This is the repository for the open source version of Event Store, which includes the clustering implementation for high availability. 
  
## Releases (.net Core)
You can download releases from https://github.com/riccardone/EventStore/releases  

## Support  
  
This fork is only for running Event Store on .net Core for development and testing purposes. It's not commercially supported and it is not intended to be used in production.  

Information on commercial support and options such as LDAP authentication can be found on the Event Store website at https://eventstore.org/support.  

## Documentation
Documentation for Event Store can be found [here](https://eventstore.org/docs/)  
  
## Building Event Store

Event Store is written in a mixture of C#, C++ and JavaScript. The version in this fork can run on .NET Core v2.2+.

### Linux
**Prerequisites**
- [.NET Core SDK 2.2](https://www.microsoft.com/net/download)

### Windows
**Prerequisites**
- [.NET Core SDK 2.2](https://www.microsoft.com/net/download)

### Mac OS X
**Prerequisites**
- [.NET Core SDK 2.2](https://www.microsoft.com/net/download)

### Build EventStore
Once you've installed the prerequisites for your system, you can launch a `Release` build of EventStore as follows:
```
dotnet build -c Release src/EventStore.sln
```

To start a single node, you can then run:
```
bin/Release/EventStore.ClusterNode/netcoreapp2.2/dotnet EventStore.ClusterNode.dll --db ../db --log ../logs
```

### Running the tests
You can launch the tests as follows:

#### EventStore Core tests
```
dotnet test src/EventStore.Core.Tests/EventStore.Core.Tests.csproj -- RunConfiguration.TargetPlatform=x64
```

#### EventStore Projections tests
```
dotnet test src/EventStore.Projections.Core.Tests/EventStore.Projections.Core.Tests.csproj -- RunConfiguration.TargetPlatform=x64
```

## Building the EventStore Client / Embedded Client
You can build the client / embedded client with the steps below. This will generate a nuget package file (.nupkg) that you can include in your project.
#### Client
```
dotnet pack -c Release src/EventStore.ClientAPI/EventStore.ClientAPI.csproj /p:Version=5.0.0
```

#### Embedded Client
```
dotnet pack -c Release src/EventStore.ClientAPI.Embedded/EventStore.ClientAPI.Embedded.csproj /p:Version=5.0.0
```


## Building the EventStore web UI
The web UI is prebuilt and the files are located under [src/EventStore.ClusterNode.Web/clusternode-web](src/EventStore.ClusterNode.Web/clusternode-web).
If you want to build the web UI, please consult this [repository](https://github.com/EventStore/EventStore.UI) which is also a git submodule of the current repository located under `src/EventStore.UI`.

## Building the Projections Library
The list of precompiled projections libraries can be found in `src/libs/x64`. If you still want to build the projections library please follow the links below.
- [Linux](scripts/build-js1/build-js1-linux/README.md)
- [Windows](scripts/build-js1/build-js1-win/build-js1-win-instructions.md)
- [Mac OS X](scripts/build-js1/build-js1-mac/build-js1-mac.sh)
