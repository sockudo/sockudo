#!/bin/bash

# exit if any command fails
set -e

dotnet restore ./PusherServer.Core/PusherServer.Core.csproj

# Instead, run directly with mono for the full .net version
dotnet build ./PusherServer.Core/PusherServer.Core.csproj -c Release