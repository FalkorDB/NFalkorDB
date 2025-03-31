[![Discord](https://img.shields.io/discord/1146782921294884966?style=flat-square)](https://discord.gg/ErBEqN9E)
[![license](https://img.shields.io/github/license/FalkorDB/NFalkorDB.svg)](https://github.com/FalkorDB/NFalkorDB/blob/master/LICENSE)
[![Release](https://img.shields.io/github/release/FalkorDB/NFalkorDB.svg)](https://github.com/FalkorDB/NFalkorDB/releases/latest)
[![Build Status](https://github.com/falkordb/NFalkorDB/actions/workflows/dotnet.yml/badge.svg)](https://github.com/falkordb/NFalkorDB/actions/workflows/dotnet.yml)

# NFalkorDB

[![Try Free](https://img.shields.io/badge/Try%20Free-FalkorDB%20Cloud-FF8101?labelColor=FDE900&style=for-the-badge&link=https://app.falkordb.cloud)](https://app.falkordb.cloud)

## Overview

NFalkorDB is a series of extensions methods for the [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis) library that will enable you to interact with the [Redis](https://redis.io) module [FalkorDB](https://www.falkordb.com). This is made possible by the `Execute` and `ExecuteAsync` methods already present in the StackExchange.Redis library.

The intent of this library is to duplicate the API (as much as possible) of the FalkorDB module support found embedded in the [Jedis](https://github.com/xetorthio/jedis) library.

## Installation

`PM> Install-Package NFalkorDB -Version 1.0.0`

## Usage

I'm assuming that you already have the [FalkorDB](https://docs.falkordb.com/) module installed on your Redis server.

You can verify that the module is installed by executing the following command:

`MODULE LIST`

If FalkorDB is installed you should see output similar to the following:

```
1) 1) "name"
   2) "graph"
   3) "ver"
   4) (integer) 20811
```

(The version of the module installed on your server obviously may vary.)

## Examples

In this repository there are a suite of integration tests that should be sufficient to serve as examples on how to use all supported FalkorDB commands.

[Integration Tests](https://github.com/falkordb/NFalkorDB/blob/master/NFalkorDB.Tests/FalkorDBAPITest.cs)

## License

NFalkorDB is licensed under the Apache-2.0 [license ](https://github.com/FalkorDB/NFalkorDB/blob/master/LICENSE).
