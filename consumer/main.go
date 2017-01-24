package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/fzzy/radix/redis"
)

var (
	key         = flag.String("key", "test_queue", "the redis list key")
	concurrency = flag.Int("c", 2, "concurrenty")
)

func errHandler(err error) {
	if err != nil {
		log.Println("error:", err)
		os.Exit(1)
	}
}

type task struct {
	name    string
	stopSig chan bool
	done    chan bool
	client  *redis.Client
	err     error
}

func (t *task) Stop() {
	t.stopSig <- true
}

// Run the task
func (t *task) Run() {
	t.client, t.err = redis.Dial("tcp", "127.0.0.1:6379")
	if t.err != nil {
		panic(t.err)
	}

	var reply *redis.Reply
	var result []string

	t.stopSig = make(chan bool)

	go func() {
		needExit := false
		for {
		goexit:
			if needExit {
				break
			}

			select {
			case <-t.stopSig:
				log.Println(t.name, " receive stop signal")
				t.err = t.client.Close()
				t.done <- true
				needExit = true
				log.Println("goto exit")
				goto goexit
			default:
				log.Println(t.name, " start to block...")
				// will block recieve one piece one time
				reply = t.client.Cmd("brpop", *key, 0)
				errHandler(reply.Err)
				result, t.err = reply.List()
				errHandler(t.err)
				for _, v := range result {
					log.Println(t.name, " received:", string(v))
				}
			}
		}
	}()
}

func (t *task) Err() error {
	return t.err
}

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lshortfile)

	stopSigs := make(chan os.Signal, 1)
	signal.Notify(stopSigs, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan bool)
	tasks := make([]*task, 0, *concurrency)

	for i := 0; i < *concurrency; i++ {
		name := fmt.Sprintf("task_%d:", i)
		task := new(task)
		task.name = name
		task.done = done

		tasks = append(tasks, task)

		task.Run()
	}

	go func() {
		sigs := <-stopSigs
		log.Println("received stop signal:", sigs)

		for i := range tasks {
			log.Println("sending task ", tasks[i].name, " to stop")
			tasks[i].Stop()
			log.Println("sending task ", tasks[i].name, " to stop done")
		}
	}()

	log.Println("listening tasks...")
	var i int
	for _ = range done {
		i++
		if i == *concurrency {
			log.Println("all tasks stoped.")
			break
		}
	}
}
