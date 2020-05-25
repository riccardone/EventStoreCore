# Event Store on .Net Core 3.1  

The open-source, functional database for Event Sourcing.

This is the repository for the open source version of Event Store, which includes the clustering implementation for high availability. 
  
## Releases (.net Core)
You can download releases from https://github.com/riccardone/EventStore/releases  

## Support  
  
This fork is is not commercially supported. You can use in production as I do at you risk.  

I will keep it aligned to the latest oss version of the upstream as frequent as I can.

Information on commercial support and options such as LDAP authentication can be found on the Event Store website at https://eventstore.org/support.  

## Documentation
Documentation for Event Store can be found [here](https://eventstore.org/docs/)  
  
## Building Event Store

Event Store is written in a mixture of C#, C++ and JavaScript. The version in this fork can run on .NET Core v2.2+.

### Build EventStore
You can launch a `Release` build of EventStore as follows:
```
dotnet build -c Release src/EventStore.sln
```

To start a single node, you can then run:
```
bin/Release/EventStore.ClusterNode/netcoreapp3.1/dotnet EventStore.ClusterNode.dll --db ../db --log ../logs
```