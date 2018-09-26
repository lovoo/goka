/*

This package provides a kafka mock that allows integration testing of goka processors.

Usage

Simply append a tester option when creating the processor for testing.
Usually it makes sense to move the processor creation to a function that accepts
extra options. That way the test can use exactly the same processor setup.

  // creates the processor defining its group graph
  func createProcessor(brokers []string, options ...goka.ProcessorOption) *goka.Processor{
    return goka.NewProcessor(brokers, goka.DefineGroup("group",
                            // some group definitions
                            options...,
                            ),
    )
  }

In the main function we would run the processor like this:
  func main(){
    proc := createProcessor([]string{"broker1:9092"})
    proc.Run(ctx.Background())
  }
And in the unit test something like:
  func TestProcessor(t *testing.T){
    // create tester
    tester := tester.New(t)
    // create the processor
    proc := createProcessor(nil, goka.WithTester(tester))

    // .. do extra initialization if necessary

    go proc.Run(ctx.Background())

    // execute the actual test
    tester.Consume("input-topic", "key", "value")

    value := tester.TableValue("group-table", "key")
    if value != expected{
      t.Fatalf("got unexpected table value")
    }
  }

See https://github.com/lovoo/goka/tree/master/examples/testing for a full example
*/
package tester
