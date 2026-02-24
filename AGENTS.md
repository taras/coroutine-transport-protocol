# Big Idea

Create a version of [Effection](https://github.com/thefrontside/effection) that uses [Durable Streams](https://github.com/durable-streams/durable-streams) under the hood. We won't be able to use the operations that come with Effection, but we can implement a version of the same operations using Durable Streams so that we can use the same syntax, with different types, that will be durable by default.

We started working on types in @types.ts.