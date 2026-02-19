export type Procedure<TArgs, TReturn, TProgress, TResume> = {};

export type Invocation<TReturn, TProgress, TResume> = {};

type Continuation<TReturn, TProgress, TResume> = {
  done: true;
  value: TReturn;
} | {
  done: false;
  progress: TProgress;
  resume: (
    value: TResume,
  ) => Operation<Continuation<TReturn, TProgress, TResume>>;
};
