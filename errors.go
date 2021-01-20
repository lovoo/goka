package goka

import (
	"errors"
	"fmt"
	reflect "reflect"
	"regexp"
	"strings"

	"github.com/go-stack/stack"
)

var (
	errBuildConsumer = "error creating Kafka consumer: %v"
	errBuildProducer = "error creating Kafka producer: %v"
	errApplyOptions  = "error applying options: %v"
	errTopicNotFound = errors.New("requested topic was not found")
)

var (
	// this regex matches the package name + some hash info, if we're in gomod but not subpackages
	// examples which match
	// * github.com/lovoo/goka/processor.go
	// * github.com/lovoo/goka@v1.0.0/view.go
	// * github.com/some-fork/goka/view.go
	// examples which do not match
	// * github.com/something/else
	// * github.com/lovoo/goka/subpackage/file.go
	// this regex is used to filter out entries from the stack trace that origin
	// from the root-package of go (but not the subpackages, otherwise we would not see the stack in the example-tests)
	// reflect.TypeOf(Processor{}).PkgPath() returns (in the main repo) "github.com/lovoo/goka"
	gokaPackageRegex = regexp.MustCompile(fmt.Sprintf(`%s(?:@[^/]+)?/[^/]+$`, reflect.TypeOf(Processor{}).PkgPath()))
)

// type to indicate that some non-transient error occurred while processing
// the message, e.g. panic, (de|en)-coding errors or invalid usage of context.
type errProcessing struct {
	wrapped error
}

func (ec *errProcessing) Error() string {
	return fmt.Sprintf("error processing message: %v", ec.wrapped)
}
func (ec *errProcessing) Unwrap() error {
	return ec.wrapped
}

func newErrProcessing(err error) error {
	return &errProcessing{
		wrapped: err,
	}
}

// userStacktrace returns a formatted stack trace only containing the stack trace of the user-code
// This is mainly used to properly format the error message built after a panic happened in a
// processor-callback.
func userStacktrace() []string {

	trace := stack.Trace()

	// pop calls from the top that are either from runtime or goka's internal functions
	for len(trace) > 0 {
		if strings.HasPrefix(fmt.Sprintf("%+s", trace[0]), "runtime/") {
			trace = trace[1:]
			continue
		}
		if gokaPackageRegex.MatchString(fmt.Sprintf("%+s", trace[0])) {
			trace = trace[1:]
			continue
		}
		break
	}

	var lines []string
	for _, frame := range trace {

		// as soon as we hit goka's internal package again we'll stop because from this point on we would
		// only print library or runtime frames
		if gokaPackageRegex.MatchString(fmt.Sprintf("%+s", frame)) {
			break
		}
		lines = append(lines, fmt.Sprintf("%n\n\t%+s:%d", frame, frame, frame))
	}

	// if we don't have anything unfiltered, it means there was an error within goka itself, so we should just
	// return the whole stack trace.
	if len(lines) == 0 {
		for _, frame := range stack.Trace() {
			lines = append(lines, fmt.Sprintf("%n\n\t%+s:%d", frame, frame, frame))
		}
	}

	return lines

}
