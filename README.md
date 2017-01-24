# Redis as Queue

此测试程序使用Redis作为队列作用，通过lpush + brpop实现，通过控制程序的-c参数来控制并发进程的数量，并且程序捕捉signal保证任务完成后退出.
