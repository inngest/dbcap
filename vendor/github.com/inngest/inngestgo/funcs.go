package inngestgo

import (
	"context"
	"reflect"
	"time"

	"github.com/gosimple/slug"
	"github.com/inngest/inngest/pkg/inngest"
)

func StrPtr(i string) *string { return &i }

func IntPtr(i int) *int { return &i }

type FunctionOpts struct {
	// ID is an optional function ID.  If not specified, the ID
	// will be auto-generated by lowercasing and slugging the name.
	ID string
	// Name represents a human-readable function name.
	Name string

	Priority    *inngest.Priority
	Concurrency []inngest.Concurrency
	Idempotency *string
	Retries     *int
	Cancel      []inngest.Cancel
	Debounce    *Debounce
	// Timeouts represents timeouts for a function.
	Timeouts *Timeouts
	// Throttle represents a soft rate limit for gating function starts.  Any function runs
	// over the throttle period will be enqueued in the backlog to run at the next available
	// time.
	Throttle *Throttle
	// RateLimit allows specifying custom rate limiting for the function.  A RateLimit is
	// hard rate limiting:  any function invocations over the rate limit will be ignored and
	// will never run.
	RateLimit *RateLimit
	// BatchEvents represents batching
	BatchEvents *inngest.EventBatchConfig
}

// GetRateLimit returns the inngest.RateLimit for function configuration.  The
// SDK's RateLimit type is incompatible with the inngest.RateLimit type signature
// for ease of definition.
func (f FunctionOpts) GetRateLimit() *inngest.RateLimit {
	if f.RateLimit == nil {
		return nil
	}
	return f.RateLimit.Convert()
}

// Debounce represents debounce configuration used when creating a new function within
// FunctionOpts
type Debounce struct {
	// Key is an optional expression to use for constraining the debounce to a given
	// value.
	Key string `json:"key,omitempty"`
	// Period is how long to listen for new events matching the optional key.  Any event
	// received during this period will reschedule the debounce to run after another period
	// interval.
	Period time.Duration `json:"period"`
	// Timeout specifies the optional max lifetime of a debounce, ensuring that functions
	// run after the given duration when a debounce is rescheduled indefinitely.
	Timeout *time.Duration `json:"timeout,omitempty"`
}

// Throttle represents concurrency over time.
type Throttle struct {
	// Limit is how often the function can be called within the specified period.  The
	// minimum limit is 1.
	Limit uint `json:"limit"`
	// Period represents the time period for throttling the function.  The minimum
	// granularity is 1 second.  Run starts are spaced evenly through the given period.
	Period time.Duration `json:"period"`
	// Burst is number of runs allowed to start in the given window, in a single burst,
	// before throttling applies.
	//
	// A burst > 1 bypasses smoothing for the burst and allows many runs to start
	// at once, if desired.  Defaults to 1, which disables bursting.
	Burst uint `json:"burst"`
	// Key is an optional string to constrain throttling using event data.  For
	// example, if you want to throttle incoming notifications based off of a user's
	// ID in an event you can use the following key: "event.user.id".  This ensures
	// that we throttle functions for each user independently.
	Key *string `json:"key,omitempty"`
}

type RateLimit struct {
	// Limit is how often the function can be called within the specified period
	Limit uint `json:"limit"`
	// Period represents the time period for throttling the function
	Period time.Duration `json:"period"`
	// Key is an optional string to constrain rate limiting using event data.  For
	// example, if you want to rate limit incoming notifications based off of a user's
	// ID in an event you can use the following key: "event.user.id".  This ensures
	// that we rate limit functions for each user independently.
	Key *string `json:"key,omitempty"`
}

// Convert converts a RateLimit to an inngest.RateLimit
func (r RateLimit) Convert() *inngest.RateLimit {
	return &inngest.RateLimit{
		Limit:  r.Limit,
		Period: r.Period.String(),
		Key:    r.Key,
	}
}

// Timeouts represents timeouts for the function. If any of the timeouts are hit, the function
// will be marked as cancelled with a cancellation reason.
type Timeouts struct {
	// Start represents the timeout for starting a function.  If the time between scheduling
	// and starting a function exceeds this value, the function will be cancelled.  Note that
	// this is inclusive of time between retries.
	//
	// A function may exceed this duration because of concurrency limits, throttling, etc.
	Start time.Duration `json:"start,omitempty"`

	// Finish represents the time between a function starting and the function finishing.
	// If a function takes longer than this time to finish, the function is marked as cancelled.
	// The start time is taken from the time that the first successful function request begins,
	// and does not include the time spent in the queue before the function starts.
	//
	// Note that if the final request to a function begins before this timeout, and completes
	// after this timeout, the function will succeed.
	Finish time.Duration `json:"finish,omitempty"`
}

// CreateFunction creates a new function which can be registered within a handler.
//
// This function uses generics, allowing you to supply the event that triggers the function.
// For example, if you have a signup event defined as a struct you can use this to strongly
// type your input:
//
//	type SignupEvent struct {
//		Name string
//		Data struct {
//			Email     string
//			AccountID string
//		}
//	}
//
//	f := CreateFunction(
//		inngestgo.FunctionOptions{Name: "Post-signup flow"},
//		inngestgo.EventTrigger("user/signed.up"),
//		func(ctx context.Context, input gosdk.Input[SignupEvent]) (any, error) {
//			// .. Your logic here.  input.Event will be strongly typed as a SignupEvent.
//			// step.Run(ctx, "Do some logic", func(ctx context.Context) (string, error) { return "hi", nil })
//		},
//	)
func CreateFunction[T any](
	fc FunctionOpts,
	trigger inngest.Trigger,
	f SDKFunction[T],
) ServableFunction {
	// Validate that the input type is a concrete type, and not an interface.
	//
	// The only exception is `any`, when users don't care about the input event
	// eg. for cron based functions.

	sf := servableFunc{
		fc:      fc,
		trigger: trigger,
		f:       f,
	}

	zt := sf.ZeroType()
	if zt.Interface() == nil && zt.NumMethod() > 0 {
		panic("You cannot use an interface type as the input within an Inngest function.")
	}
	return sf
}

func EventTrigger(name string, expression *string) inngest.Trigger {
	return inngest.Trigger{
		EventTrigger: &inngest.EventTrigger{
			Event:      name,
			Expression: expression,
		},
	}
}

func CronTrigger(cron string) inngest.Trigger {
	return inngest.Trigger{
		CronTrigger: &inngest.CronTrigger{
			Cron: cron,
		},
	}
}

// SDKFunction represents a user-defined function to be called based off of events or
// on a schedule.
//
// The function is registered with the SDK by calling `CreateFunction` with the function
// name, the trigger, the event type for marshalling, and any options.
//
// This uses generics to strongly type input events:
//
//	func(ctx context.Context, input gosdk.Input[SignupEvent]) (any, error) {
//		// .. Your logic here.  input.Event will be strongly typed as a SignupEvent.
//	}
type SDKFunction[T any] func(ctx context.Context, input Input[T]) (any, error)

// ServableFunction defines a function which can be called by a handler's Serve method.
//
// This is created via CreateFunction in this package.
type ServableFunction interface {
	// Slug returns the function's human-readable ID, such as "sign-up-flow".
	Slug() string

	// Name returns the function name.
	Name() string

	Config() FunctionOpts

	// Trigger returns the event name or schedule that triggers the function.
	Trigger() inngest.Trigger

	// ZeroEvent returns the zero event type to marshal the event into, given an
	// event name.
	ZeroEvent() any

	// Func returns the SDK function to call.  This must alawys be of type SDKFunction,
	// but has an any type as we register many functions of different types into a
	// type-agnostic handler; this is a generic implementation detail, unfortunately.
	Func() any
}

// Input is the input data passed to your function.  It is comprised of the triggering event
// and call context.
type Input[T any] struct {
	Event    T        `json:"event"`
	Events   []T      `json:"events"`
	InputCtx InputCtx `json:"ctx"`
}

type InputCtx struct {
	Env        string `json:"env"`
	FunctionID string `json:"fn_id"`
	RunID      string `json:"run_id"`
	StepID     string `json:"step_id"`
	Attempt    int    `json:"attempt"`
}

type servableFunc struct {
	fc      FunctionOpts
	trigger inngest.Trigger
	f       any
}

func (s servableFunc) Config() FunctionOpts {
	return s.fc
}

func (s servableFunc) Slug() string {
	if s.fc.ID == "" {
		return slug.Make(s.fc.Name)
	}
	return s.fc.ID
}

func (s servableFunc) Name() string {
	return s.fc.Name
}

func (s servableFunc) Trigger() inngest.Trigger {
	return s.trigger
}

func (s servableFunc) ZeroType() reflect.Value {
	// Grab the concrete type from the generic Input[T] type.  This lets us easily
	// initialize new values of this type at runtime.
	fVal := reflect.ValueOf(s.f)
	inputVal := reflect.New(fVal.Type().In(1)).Elem()
	return reflect.New(inputVal.FieldByName("Event").Type()).Elem()
}

func (s servableFunc) ZeroEvent() any {
	return s.ZeroType().Interface()
}

func (s servableFunc) Func() any {
	return s.f
}
