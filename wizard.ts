// server

type Workflow<T> = Iterable<DurableEffect, T>;

function* serverLoop(): Workflow<never> {
  for (let connection of yield* each(connections)) {
    let continuation = yield* elicit(Wizard, defaults);

    // while (!yield* continuation.done()) {
    //   let validation = validate(continuation.progress);
    //   if (validation.ok) {
    // 	yield* doSomemthing(validation.value);
    // 	break;
    //   }
    //   yield* continuation.set(yield* continuation.resume(validation)); // <-
    // }
  }
}
