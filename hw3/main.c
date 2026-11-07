#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <mqueue.h>
#include <poll.h>
#include <semaphore.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define DEPTS 3
#define BUYERS 100
#define ACTORS (BUYERS + DEPTS)
#define INPUT_SIZE 65536
#define NEVER INT64_MAX

enum Mode
{
    FORK_NAMED = 1,
    FORK_UNNAMED,
    SHM,
    MQ
};

enum Action
{
    ENTER = 1,
    NEXT,
    START,
    FINISH,
    REJECT,
    STOP
};

enum State
{
    FUTURE,
    WAITING,
    SERVING,
    LEFT,
    REJECTED
};

// Задание адресовано одному покупателю или продавцу
struct Task
{
    int action, actor;
    int length, route[DEPTS];
    int min, max;
    unsigned int seed;
};

// Каждый процесс отправляет один ответ на полученное задание
struct Reply
{
    int actor, status, department, duration;
};

// Общая память используется только в первых трёх режимах
struct Shared
{
    struct Task task;
    struct Reply reply;
    unsigned char joined[ACTORS];
    sem_t ready, done, gate[ACTORS];
};

struct Buyer
{
    int route[DEPTS], length, position;
    enum State state;
};

struct Dept
{
    int min, max;
    int queue[BUYERS], head, tail;
    int buyer, purchases;
    int64_t finish;
};

// Очереди и модельное время изменяет только управляющий процесс
struct Shop
{
    int count, interval, closing, delay;
    int closed, fd, error;
    unsigned int seed;
    int64_t now;
    struct Buyer buyers[BUYERS];
    struct Dept depts[DEPTS];
};

// Здесь собраны ресурсы всех четырёх способов обмена
struct Ipc
{
    enum Mode mode;
    char token[32];
    int count, fd, children_count, owner;
    struct Shared *shared;
    sem_t *ready, *done, *gate[ACTORS];
    mqd_t tasks, replies;
    pid_t children[ACTORS];
    unsigned char joined[ACTORS];
};

struct Worker
{
    int actor, length, position;
    int route[DEPTS], seeded;
    unsigned int random;
};

static volatile sig_atomic_t stopping;

// Обработчик только запоминает сигнал, остальная работа идёт в main
static void on_signal(int number)
{
    stopping = number;
}

static enum Mode mode_of(const char *name)
{
    if (!strcmp(name, "fork-named"))
    {
        return FORK_NAMED;
    }
    if (!strcmp(name, "fork-unnamed"))
    {
        return FORK_UNNAMED;
    }
    if (!strcmp(name, "shm"))
    {
        return SHM;
    }
    if (!strcmp(name, "mq"))
    {
        return MQ;
    }
    return 0;
}

static void ipc_name(char *out, const char *token, const char *type, int actor)
{
    if (actor < 0)
    {
        snprintf(out, 80, "/ak3_%s_%s", token, type);
    }
    else
    {
        snprintf(out, 80, "/ak3_%s_g%03d", token, actor);
    }
}

static int number(char **text, long min, long max, long *value)
{
    while (**text == ' ' || **text == '\n' || **text == '\r' || **text == '\t')
    {
        ++*text;
    }
    errno = 0;
    char *end;
    long n = strtol(*text, &end, 10);
    if (end == *text || errno || n < min || n > max ||
        (*end && *end != ' ' && *end != '\n' && *end != '\r' && *end != '\t'))
    {
        return -1;
    }
    *text = end;
    *value = n;
    return 0;
}

// Проверяет аргумент целиком, без лишних символов
static int argument(char *text, long max, long *value)
{
    char *p = text;
    return number(&p, 0, max, value) == 0 && *p == '\0' ? 0 : -1;
}

// Формат входных данных остался таким же, как в ИДЗ1 и ИДЗ2
static int load(const char *path, struct Shop *s, struct stat *info)
{
    char data[INPUT_SIZE];
    int fd = open(path, O_RDONLY | O_NONBLOCK);
    size_t used = 0;
    if (fd < 0)
    {
        perror("Входной файл");
        return -1;
    }
    if (fstat(fd, info) || !S_ISREG(info->st_mode))
    {
        dprintf(STDERR_FILENO, "Вход должен быть обычным файлом\n");
        close(fd);
        return -1;
    }
    while (used < sizeof(data) - 1)
    {
        ssize_t n = read(fd, data + used, sizeof(data) - 1 - used);
        if (n < 0 && errno == EINTR)
        {
            continue;
        }
        if (n <= 0)
        {
            if (n < 0)
            {
                perror("Чтение входа");
                close(fd);
                return -1;
            }
            break;
        }
        used += (size_t)n;
    }
    int invalid = used == sizeof(data) - 1 || memchr(data, '\0', used) != NULL;
    if (close(fd) || invalid)
    {
        dprintf(STDERR_FILENO, "Некорректный входной файл\n");
        return -1;
    }
    data[used] = '\0';
    char *p = data;
    long n;
    if (number(&p, 0, BUYERS, &n))
    {
        goto bad_input;
    }
    s->count = (int)n;
    if (number(&p, 0, 100000, &n))
    {
        goto bad_input;
    }
    s->interval = (int)n;
    if (number(&p, 0, 100000, &n))
    {
        goto bad_input;
    }
    s->closing = (int)n;

    for (int d = 0; d < DEPTS; d++)
    {
        struct Dept *dep = &s->depts[d];
        if (number(&p, 1, 10000, &n))
        {
            goto bad_input;
        }
        dep->min = (int)n;
        if (number(&p, dep->min, 10000, &n))
        {
            goto bad_input;
        }
        dep->max = (int)n;
        dep->buyer = -1;
    }
    for (int i = 0; i < s->count; i++)
    {
        struct Buyer *b = &s->buyers[i];
        if (number(&p, 1, DEPTS, &n))
        {
            goto bad_input;
        }
        b->length = (int)n;
        for (int j = 0; j < b->length; j++)
        {
            if (number(&p, 1, DEPTS, &n))
            {
                goto bad_input;
            }
            b->route[j] = (int)n - 1;
            for (int k = 0; k < j; k++)
            {
                if (b->route[k] == b->route[j])
                {
                    goto bad_input;
                }
            }
        }
    }
    while (*p == ' ' || *p == '\n' || *p == '\t' || *p == '\r')
    {
        p++;
    }
    if (*p == '\0')
    {
        return 0;
    }
bad_input:
    dprintf(STDERR_FILENO, "Некорректные данные во входном файле\n");
    return -1;
}

// Журнал нельзя открыть поверх исходного файла, даже через ссылку
static int open_log(const char *path, const struct stat *input)
{
    struct stat info;
    int fd = open(path, O_WRONLY | O_CREAT | O_NONBLOCK, 0644);
    if (fd < 0)
    {
        perror("Журнал");
        return -1;
    }
    if (fstat(fd, &info) || !S_ISREG(info.st_mode) ||
        (info.st_dev == input->st_dev && info.st_ino == input->st_ino) ||
        ftruncate(fd, 0))
    {
        dprintf(STDERR_FILENO, "Журнал не должен совпадать с входным файлом\n");
        close(fd);
        return -1;
    }
    return fd;
}

// Неблокирующий вывод позволяет закончить программу по SIGINT
static int write_text(int fd, const char *text, size_t size)
{
    for (size_t used = 0; used < size;)
    {
        if (fd == STDOUT_FILENO && stopping)
        {
            return 0;
        }
        ssize_t n = write(fd, text + used, size - used);
        if (n > 0)
        {
            used += (size_t)n;
        }
        else if (n < 0 && errno == EINTR)
        {
            continue;
        }
        else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            struct pollfd p = { .fd = fd, .events = POLLOUT };
            if (poll(&p, 1, 50) < 0 && errno != EINTR)
            {
                return -1;
            }
        }
        else
        {
            return -1;
        }
    }
    return 0;
}

// Все события выводит управляющий процесс, строки не перемешиваются
static void event(struct Shop *s, const char *format, ...)
{
    char message[1024], line[1120];
    va_list args;
    va_start(args, format);
    int n = vsnprintf(message, sizeof(message), format, args);
    va_end(args);
    if (n < 0 || (size_t)n >= sizeof(message))
    {
        s->error = 1;
        return;
    }
    n = snprintf(line, sizeof(line), "[t=%" PRId64 "] %s\n", s->now, message);
    if (n < 0 || (size_t)n >= sizeof(line))
    {
        s->error = 1;
        return;
    }
    int terminal = write_text(STDOUT_FILENO, line, (size_t)n);
    int journal = write_text(s->fd, line, (size_t)n);
    if (terminal < 0 || journal < 0)
    {
        s->error = 1;
    }
}

// Покупатель помнит маршрут, а продавец выбирает время обслуживания
static void handle(struct Worker *w, const struct Task *task, struct Reply *reply)
{
    *reply = (struct Reply){ .actor = w->actor, .department = -1, .status = -1 };
    if (task->actor != w->actor)
    {
        return;
    }
    switch (task->action)
    {
        case ENTER:
            if (w->actor < DEPTS || task->length < 1 || task->length > DEPTS)
            {
                break;
            }
            w->length = task->length;
            w->position = 0;
            memcpy(w->route, task->route, sizeof(w->route));
            reply->department = w->route[0];
            break;
        case NEXT:
            if (w->actor < DEPTS || !w->length)
            {
                return;
            }
            w->position++;
            if (w->position < w->length)
            {
                reply->department = w->route[w->position];
            }
            break;
        case START:
            if (w->actor >= DEPTS || task->min < 1 || task->max < task->min)
            {
                return;
            }
            if (!w->seeded)
            {
                w->random = task->seed ^ (0x9e3779b9U * (unsigned)(w->actor + 1));
                w->seeded = 1;
            }
            reply->duration = task->min;
            if (task->min < task->max)
            {
                reply->duration += (int)((unsigned)rand_r(&w->random) %
                    (unsigned)(task->max - task->min + 1));
            }
            break;
        case FINISH:
            if (w->actor >= DEPTS)
            {
                return;
            }
            break;
        case REJECT:
            if (w->actor < DEPTS)
            {
                return;
            }
            break;
        case STOP:
            break;
        default:
            return;
    }
    reply->status = 0;
}

// Ожидание с ограничением по времени защищает от зависания участника
static int wait_sem(sem_t *sem, int seconds, int check_signal)
{
    struct timespec until;
    if (clock_gettime(CLOCK_REALTIME, &until))
    {
        return -1;
    }
    until.tv_sec += seconds;
    for (;;)
    {
        if (check_signal && stopping)
        {
            errno = EINTR;
            return -1;
        }
        if (!sem_timedwait(sem, &until))
        {
            return 0;
        }
        if (errno != EINTR)
        {
            return -1;
        }
    }
}

static int send_message(mqd_t queue, const void *message, size_t size)
{
    int result;
    do
    {
        result = mq_send(queue, message, size, 0);
    }
    while (result && errno == EINTR);
    return result;
}

static void ipc_empty(struct Ipc *c, enum Mode mode, const char *token)
{
    memset(c, 0, sizeof(*c));
    c->mode = mode;
    snprintf(c->token, sizeof(c->token), "%s", token);
    c->fd = -1;
    c->tasks = c->replies = (mqd_t)-1;
    c->ready = c->done = SEM_FAILED;
    for (int i = 0; i < ACTORS; i++)
    {
        c->gate[i] = SEM_FAILED;
    }
}

// Родитель создаёт общий набор IPC-объектов для выбранного режима
static int ipc_create(struct Ipc *c, enum Mode mode, int count)
{
    char token[32], name[80];
    snprintf(token, sizeof(token), "%ld", (long)getpid());
    ipc_empty(c, mode, token);
    c->count = count;
    c->owner = 1;
    if (mode != MQ)
    {
        ipc_name(name, token, "mem", -1);
        c->fd = shm_open(name, O_CREAT | O_EXCL | O_RDWR, 0600);
        if (c->fd < 0 || ftruncate(c->fd, sizeof(struct Shared)))
        {
            return -1;
        }
        c->shared = mmap(NULL, sizeof(struct Shared), PROT_READ | PROT_WRITE,
            MAP_SHARED, c->fd, 0);
        if (c->shared == MAP_FAILED)
        {
            c->shared = NULL;
            return -1;
        }
        memset(c->shared, 0, sizeof(struct Shared));
    }
    if (mode == FORK_UNNAMED)
    {
        if (sem_init(&c->shared->done, 1, 0))
        {
            return -1;
        }
        c->done = &c->shared->done;
        for (int i = 0; i < count; i++)
        {
            if (sem_init(&c->shared->gate[i], 1, 0))
            {
                return -1;
            }
            c->gate[i] = &c->shared->gate[i];
        }
        if (sem_init(&c->shared->ready, 1, 0))
        {
            return -1;
        }
        c->ready = &c->shared->ready;
    }
    else
    {
        ipc_name(name, token, "done", -1);
        c->done = sem_open(name, O_CREAT | O_EXCL, 0600, 0);
        if (c->done == SEM_FAILED)
        {
            return -1;
        }
        for (int i = 0; i < count; i++)
        {
            ipc_name(name, token, "gate", i);
            c->gate[i] = sem_open(name, O_CREAT | O_EXCL, 0600, 0);
            if (c->gate[i] == SEM_FAILED)
            {
                return -1;
            }
        }
        if (mode == MQ)
        {
            struct mq_attr attr = { .mq_maxmsg = 4, .mq_msgsize = sizeof(struct Task) };
            ipc_name(name, token, "tasks", -1);
            c->tasks = mq_open(name, O_CREAT | O_EXCL | O_RDWR, 0600, &attr);
            attr.mq_msgsize = sizeof(struct Reply);
            ipc_name(name, token, "replies", -1);
            c->replies = mq_open(name, O_CREAT | O_EXCL | O_RDWR, 0600, &attr);
            if (c->tasks == (mqd_t)-1 || c->replies == (mqd_t)-1)
            {
                return -1;
            }
        }
        // Семафор ready создаётся последним, когда остальные ресурсы готовы
        ipc_name(name, token, "ready", -1);
        c->ready = sem_open(name, O_CREAT | O_EXCL, 0600, 0);
        if (c->ready == SEM_FAILED)
        {
            return -1;
        }
    }
    return 0;
}

// Участник независимого запуска ждёт появления сеанса и подключается к нему
static int ipc_connect(struct Ipc *c, enum Mode mode, const char *token, int actor)
{
    char name[80];
    ipc_empty(c, mode, token);
    ipc_name(name, token, "ready", -1);
    for (int i = 0; i < 300; i++)
    {
        c->ready = sem_open(name, 0);
        if (c->ready != SEM_FAILED)
        {
            break;
        }
        if (errno != ENOENT)
        {
            return -1;
        }
        struct timespec pause = { .tv_nsec = 20000000L };
        nanosleep(&pause, NULL);
    }
    if (c->ready == SEM_FAILED)
    {
        return -1;
    }
    ipc_name(name, token, "done", -1);
    c->done = sem_open(name, 0);
    ipc_name(name, token, "gate", actor);
    c->gate[actor] = sem_open(name, 0);
    if (c->done == SEM_FAILED || c->gate[actor] == SEM_FAILED)
    {
        return -1;
    }
    if (mode == MQ)
    {
        ipc_name(name, token, "tasks", -1);
        c->tasks = mq_open(name, O_RDONLY);
        ipc_name(name, token, "replies", -1);
        c->replies = mq_open(name, O_WRONLY);
        return c->tasks == (mqd_t)-1 || c->replies == (mqd_t)-1 ? -1 : 0;
    }
    ipc_name(name, token, "mem", -1);
    c->fd = shm_open(name, O_RDWR, 0600);
    if (c->fd < 0)
    {
        return -1;
    }
    c->shared = mmap(NULL, sizeof(struct Shared), PROT_READ | PROT_WRITE,
        MAP_SHARED, c->fd, 0);
    if (c->shared == MAP_FAILED)
    {
        c->shared = NULL;
        return -1;
    }
    return 0;
}

// Сначала закрываем дескрипторы, потом удаляем имена ресурсов родителя
static void ipc_close(struct Ipc *c)
{
    char name[80];
    if (c->mode == FORK_UNNAMED)
    {
        for (int i = 0; i < c->count; i++)
        {
            if (c->gate[i] != SEM_FAILED)
            {
                sem_destroy(c->gate[i]);
            }
        }
        if (c->ready != SEM_FAILED)
        {
            sem_destroy(c->ready);
        }
        if (c->done != SEM_FAILED)
        {
            sem_destroy(c->done);
        }
    }
    else
    {
        for (int i = 0; i < ACTORS; i++)
        {
            if (c->gate[i] != SEM_FAILED)
            {
                sem_close(c->gate[i]);
            }
        }
        if (c->ready != SEM_FAILED)
        {
            sem_close(c->ready);
        }
        if (c->done != SEM_FAILED)
        {
            sem_close(c->done);
        }
    }
    if (c->shared)
    {
        munmap(c->shared, sizeof(struct Shared));
    }
    if (c->fd >= 0)
    {
        close(c->fd);
    }
    if (c->tasks != (mqd_t)-1)
    {
        mq_close(c->tasks);
    }
    if (c->replies != (mqd_t)-1)
    {
        mq_close(c->replies);
    }
    if (!c->owner)
    {
        return;
    }
    if (c->mode != FORK_UNNAMED)
    {
        for (int i = 0; i < c->count; i++)
        {
            ipc_name(name, c->token, "gate", i);
            sem_unlink(name);
        }
        ipc_name(name, c->token, "ready", -1);
        sem_unlink(name);
        ipc_name(name, c->token, "done", -1);
        sem_unlink(name);
    }
    ipc_name(name, c->token, "mem", -1);
    shm_unlink(name);
    ipc_name(name, c->token, "tasks", -1);
    mq_unlink(name);
    ipc_name(name, c->token, "replies", -1);
    mq_unlink(name);
}

// После каждого запроса управляющий процесс ждёт подтверждения
static int ipc_call(struct Ipc *c, const struct Task *task, struct Reply *reply, int timeout)
{
    int actor = task->actor;
    if (actor < 0 || actor >= c->count || !c->joined[actor])
    {
        return -1;
    }
    if (c->mode == MQ)
    {
        if (send_message(c->tasks, task, sizeof(*task)))
        {
            return -1;
        }
    }
    else
    {
        c->shared->task = *task;
    }
    if (sem_post(c->gate[actor]) || wait_sem(c->done, timeout, task->action != STOP))
    {
        return -1;
    }
    if (c->mode == MQ)
    {
        if (mq_receive(c->replies, (char *)reply, sizeof(*reply), NULL) !=
            (ssize_t)sizeof(*reply)) return -1;
    }
    else
    {
        *reply = c->shared->reply;
    }
    return reply->actor == actor && reply->status == 0 ? 0 : -1;
}

// Процессы сообщают о готовности перед первым модельным событием
static int ipc_wait_all(struct Ipc *c)
{
    for (int n = 0; n < c->count; n++)
    {
        if (wait_sem(c->ready, 30, 1))
        {
            return -1;
        }
        int actor = -1;
        if (c->mode == MQ)
        {
            struct Reply reply;
            if (mq_receive(c->replies, (char *)&reply, sizeof(reply), NULL) !=
                (ssize_t)sizeof(reply) || reply.status != 77) return -1;
            actor = reply.actor;
        }
        else
        {
            for (int i = 0; i < c->count; i++)
            {
                if (c->shared->joined[i] && !c->joined[i])
                {
                    actor = i;
                    break;
                }
            }
        }
        if (actor < 0 || actor >= c->count || c->joined[actor])
        {
            return -1;
        }
        c->joined[actor] = 1;
    }
    return 0;
}

// Покупатель и продавец выполняют одинаковый цикл обмена сообщениями
static int worker_loop(struct Ipc *c, int id)
{
    struct Worker w = { .actor = id };
    if (c->mode == MQ)
    {
        struct Reply registration = { .actor = id, .status = 77 };
        if (send_message(c->replies, &registration, sizeof(registration)))
        {
            return 1;
        }
    }
    else
    {
        c->shared->joined[id] = 1;
    }
    if (sem_post(c->ready))
    {
        return 1;
    }

    for (;;)
    {
        if (wait_sem(c->gate[id], 2, 0))
        {
            if (errno != ETIMEDOUT)
            {
                return 1;
            }
            long pid;
            char *end;
            errno = 0;
            pid = strtol(c->token, &end, 10);
            if (errno || *end || pid < 2 || (kill((pid_t)pid, 0) < 0 && errno == ESRCH))
            {
                return 1;
            }
            continue;
        }
        struct Task task;
        struct Reply reply;
        if (c->mode == MQ)
        {
            if (mq_receive(c->tasks, (char *)&task, sizeof(task), NULL) !=
                (ssize_t)sizeof(task)) return 1;
        }
        else
        {
            task = c->shared->task;
        }
        handle(&w, &task, &reply);
        if (c->mode == MQ)
        {
            if (send_message(c->replies, &reply, sizeof(reply)))
            {
                return 1;
            }
        }
        else
        {
            c->shared->reply = reply;
        }
        if (sem_post(c->done))
        {
            return 1;
        }
        if (task.action == STOP)
        {
            return 0;
        }
    }
}

// Дочерние процессы получают уже открытые IPC-объекты после fork
static int ipc_fork(struct Ipc *c)
{
    for (int i = 0; i < c->count; i++)
    {
        pid_t pid = fork();
        if (pid < 0)
        {
            return -1;
        }
        if (pid == 0)
        {
            struct sigaction ignore = { .sa_handler = SIG_IGN };
            sigemptyset(&ignore.sa_mask);
            sigaction(SIGINT, &ignore, NULL);
            sigaction(SIGTERM, &ignore, NULL);
            _exit(worker_loop(c, i));
        }
        c->children[c->children_count++] = pid;
    }
    return 0;
}

// Отправляем STOP и дожидаемся завершения дочерних процессов
static int ipc_stop(struct Ipc *c)
{
    int failed = 0;
    for (int i = 0; i < c->count; i++)
    {
        if (!c->joined[i])
        {
            continue;
        }
        struct Task task = { .action = STOP, .actor = i };
        struct Reply reply;
        if (ipc_call(c, &task, &reply, 2))
        {
            failed = 1;
        }
    }
    for (int i = 0; i < c->children_count; i++)
    {
        if (failed || !c->joined[i])
        {
            kill(c->children[i], SIGKILL);
        }
        int status;
        if (waitpid(c->children[i], &status, 0) < 0 ||
            !WIFEXITED(status) || WEXITSTATUS(status)) failed = 1;
    }
    return failed ? -1 : 0;
}

// Покупатель встаёт в конец очереди выбранного отдела
static void enqueue(struct Shop *s, int buyer, int department)
{
    struct Dept *d = &s->depts[department];
    event(s, "Переход: покупатель=%d отдел=%d", buyer + 1, department + 1);
    if (d->buyer >= 0 || d->head < d->tail)
    {
        event(s, "Очередь: покупатель=%d отдел=%d позиция=%d",
            buyer + 1, department + 1, d->tail - d->head + 1);
    }
    d->queue[d->tail++] = buyer;
    s->buyers[buyer].state = WAITING;
}

// При закрытии новые покупатели уже не могут войти
static int close_entrance(struct Shop *s, struct Ipc *c)
{
    s->closed = 1;
    event(s, "Вход закрыт");
    for (int i = 0; i < s->count; i++)
    {
        if (s->buyers[i].state != FUTURE)
        {
            continue;
        }
        struct Task task = { .action = REJECT, .actor = DEPTS + i };
        struct Reply reply;
        if (ipc_call(c, &task, &reply, 15))
        {
            return -1;
        }
        s->buyers[i].state = REJECTED;
        event(s, "Отказ: покупатель=%d причина=вход_закрыт плановое_прибытие=%" PRId64,
            i + 1, (int64_t)i * s->interval);
    }
    return 0;
}

// Сначала завершаем текущие покупки, затем переводим покупателей дальше
static int finish_sales(struct Shop *s, struct Ipc *c)
{
    for (int d = 0; d < DEPTS; d++)
    {
        struct Dept *dep = &s->depts[d];
        if (dep->buyer < 0 || dep->finish != s->now)
        {
            continue;
        }
        int who = dep->buyer;
        struct Task task = { .action = FINISH, .actor = d };
        struct Reply reply;
        if (ipc_call(c, &task, &reply, 15))
        {
            return -1;
        }
        event(s, "Конец: покупатель=%d отдел=%d", who + 1, d + 1);
        dep->buyer = -1;
        dep->purchases++;
        struct Buyer *b = &s->buyers[who];
        b->position++;
        task = (struct Task){ .action = NEXT, .actor = DEPTS + who };
        if (ipc_call(c, &task, &reply, 15))
        {
            return -1;
        }
        if (b->position == b->length)
        {
            if (reply.department != -1)
            {
                return -1;
            }
            b->state = LEFT;
            event(s, "Выход: покупатель=%d план=выполнен", who + 1);
        }
        else
        {
            if (reply.department != b->route[b->position])
            {
                return -1;
            }
            enqueue(s, who, reply.department);
        }
    }
    return 0;
}

// Приходящих в один такт покупателей пропускаем по их номерам
static int arrivals(struct Shop *s, struct Ipc *c)
{
    for (int i = 0; i < s->count; i++)
    {
        struct Buyer *b = &s->buyers[i];
        if (b->state != FUTURE || (int64_t)i * s->interval != s->now)
        {
            continue;
        }
        struct Task task = { .action = ENTER, .actor = DEPTS + i, .length = b->length };
        memcpy(task.route, b->route, sizeof(task.route));
        struct Reply reply;
        if (ipc_call(c, &task, &reply, 15) || reply.department != b->route[0])
        {
            return -1;
        }
        char plan[16] = "";
        size_t used = 0;
        for (int j = 0; j < b->length; j++)
        {
            int n = snprintf(plan + used, sizeof(plan) - used, "%s%d",
                j ? "->" : "", b->route[j] + 1);
            if (n < 0 || (size_t)n >= sizeof(plan) - used)
            {
                return -1;
            }
            used += (size_t)n;
        }
        event(s, "Вход: покупатель=%d", i + 1);
        event(s, "План: покупатель=%d отделы=%s", i + 1, plan);
        enqueue(s, i, reply.department);
    }
    return 0;
}

// Свободный продавец берёт первого покупателя из своей очереди
static int start_sales(struct Shop *s, struct Ipc *c)
{
    for (int d = 0; d < DEPTS; d++)
    {
        struct Dept *dep = &s->depts[d];
        if (dep->buyer >= 0 || dep->head == dep->tail)
        {
            continue;
        }
        struct Task task = {
            .action = START, .actor = d, .min = dep->min,
            .max = dep->max, .seed = s->seed
        };
        struct Reply reply;
        if (ipc_call(c, &task, &reply, 15) ||
            reply.duration < dep->min || reply.duration > dep->max)
        {
            return -1;
        }
        int who = dep->queue[dep->head++];
        dep->buyer = who;
        dep->finish = s->now + reply.duration;
        s->buyers[who].state = SERVING;
        event(s, "Начало: покупатель=%d отдел=%d окончание=%" PRId64,
            who + 1, d + 1, dep->finish);
    }
    return 0;
}

// Задержка влияет только на отображение, не на модельное время
static int pause_ticks(int64_t ticks, int delay)
{
    int64_t ms = ticks * delay;
    struct timespec left = { .tv_sec = (time_t)(ms / 1000),
        .tv_nsec = (long)(ms % 1000) * 1000000L };
    while (nanosleep(&left, &left))
    {
        if (errno != EINTR)
        {
            return -1;
        }
        if (stopping)
        {
            break;
        }
    }
    return 0;
}

// Управляющий процесс выбирает ближайшее событие и продвигает время
static int simulate(struct Shop *s, struct Ipc *c)
{
    event(s, "Запуск: отделов=3 продавцов_на_отдел=1 покупателей=%d интервал=%d закрытие=%d задержка_мс=%d seed=%u",
        s->count, s->interval, s->closing, s->delay, s->seed);
    for (int d = 0; d < DEPTS; d++)
    {
        event(s, "Параметры: отдел=%d минимум=%d максимум=%d",
            d + 1, s->depts[d].min, s->depts[d].max);
    }
    while (!stopping && !s->error)
    {
        int64_t next = s->closed ? NEVER : s->closing;
        if (!s->closed)
        {
            for (int i = 0; i < s->count; i++)
            {
                int64_t time = (int64_t)i * s->interval;
                if (s->buyers[i].state == FUTURE && time < next)
                {
                    next = time;
                }
            }
        }
        for (int d = 0; d < DEPTS; d++)
        {
            struct Dept *dep = &s->depts[d];
            if (dep->buyer >= 0 && dep->finish < next)
            {
                next = dep->finish;
            }
        }
        if (next == NEVER)
        {
            return 0;
        }
        if (pause_ticks(next - s->now, s->delay))
        {
            return -1;
        }
        if (stopping)
        {
            return 0;
        }
        s->now = next;
        if (!s->closed && s->now == s->closing && close_entrance(s, c))
        {
            return -1;
        }
        if (finish_sales(s, c))
        {
            return -1;
        }
        if (!s->closed && arrivals(s, c))
        {
            return -1;
        }
        if (start_sales(s, c))
        {
            return -1;
        }
    }
    return 0;
}

// В конце выводим общую статистику и незавершённые маршруты
static void summary(struct Shop *s, const char *reason)
{
    int entered = 0, left = 0, rejected = 0, future = 0;
    for (int i = 0; i < s->count; i++)
    {
        enum State state = s->buyers[i].state;
        if (state == FUTURE)
        {
            future++;
        }
        else if (state == REJECTED)
        {
            rejected++;
        }
        else
        {
            entered++;
            if (state == LEFT)
            {
                left++;
            }
        }
    }
    event(s, "Итог: причина=%s", reason);
    event(s, "Покупатели: всего=%d вошли=%d вышли=%d внутри=%d не_допущены=%d ещё_не_пришли=%d",
        s->count, entered, left, entered - left, rejected, future);
    for (int d = 0; d < DEPTS; d++)
    {
        struct Dept *dep = &s->depts[d];
        event(s, "Отдел: номер=%d покупок=%d очередь=%d обслуживается=%d",
            d + 1, dep->purchases, dep->tail - dep->head, dep->buyer + 1);
    }
    for (int i = 0; i < s->count; i++)
    {
        struct Buyer *b = &s->buyers[i];
        if (b->state == WAITING || b->state == SERVING)
        {
            event(s, "Остался: покупатель=%d состояние=%s отдел=%d выполнено=%d из=%d",
                i + 1, b->state == WAITING ? "очередь" : "обслуживание",
                b->route[b->position] + 1, b->position, b->length);
        }
    }
}

// Для fork-режимов процессы создаются здесь, для shm и mq запускаются отдельно
int server(enum Mode mode, char **args)
{
    long delay, seed;
    if (argument(args[2], 1000, &delay) || argument(args[3], INT_MAX, &seed))
    {
        dprintf(STDERR_FILENO, "Задержка: 0..1000, SEED: 0..INT_MAX\n");
        return 1;
    }
    struct Shop *s = calloc(1, sizeof(*s));
    if (!s)
    {
        return 1;
    }
    s->delay = (int)delay;
    s->seed = (unsigned int)seed;
    struct stat input;
    if (load(args[0], s, &input))
    {
        free(s);
        return 1;
    }
    s->fd = open_log(args[1], &input);
    if (s->fd < 0)
    {
        free(s);
        return 1;
    }
    struct sigaction sa = { .sa_handler = on_signal };
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, NULL);

    // При заполненном канале stdout сигнал не должен теряться
    struct stat out;
    int flags = -1, error = 0;
    if (!fstat(STDOUT_FILENO, &out) && S_ISFIFO(out.st_mode))
    {
        flags = fcntl(STDOUT_FILENO, F_GETFL);
        if (flags < 0 || fcntl(STDOUT_FILENO, F_SETFL, flags | O_NONBLOCK))
        {
            dprintf(STDERR_FILENO, "Не удалось настроить вывод\n");
            close(s->fd);
            free(s);
            return 1;
        }
    }
    struct Ipc ipc;
    if (ipc_create(&ipc, mode, s->count + DEPTS))
    {
        perror("Создание IPC");
        error = 1;
    }
    else
    {
        if ((mode == FORK_NAMED || mode == FORK_UNNAMED) && ipc_fork(&ipc))
        {
            perror("fork");
            error = 1;
        }
        if (!error && !stopping && ipc_wait_all(&ipc))
        {
            perror("Ожидание участников");
            error = 1;
        }
        if (!error && !stopping && simulate(s, &ipc))
        {
            dprintf(STDERR_FILENO, "Ошибка обмена между процессами\n");
            error = 1;
        }
    }
    const char *reason = stopping == SIGINT ? "SIGINT" :
        stopping == SIGTERM ? "SIGTERM" : s->error ? "ошибка_ввода_вывода" :
        error ? "ошибка_взаимодействия" : "завершено";
    summary(s, reason);
    if (ipc_stop(&ipc))
    {
        error = 1;
    }
    ipc_close(&ipc);
    if (flags >= 0 && fcntl(STDOUT_FILENO, F_SETFL, flags))
    {
        error = 1;
    }
    if (close(s->fd))
    {
        error = 1;
    }
    if (s->error)
    {
        error = 1;
    }
    free(s);
    return stopping ? 128 + stopping : error ? 1 : 0;
}

// Участник независимого запуска получает роль и номер из аргументов
int independent_actor(enum Mode mode, char **args)
{
    long pid, number;
    if (argument(args[0], INT_MAX, &pid) || pid < 2 ||
        argument(args[2], BUYERS, &number) || number < 1)
    {
        return 1;
    }
    int actor;
    if (!strcmp(args[1], "seller") && number <= DEPTS)
    {
        actor = (int)number - 1;
    }
    else if (!strcmp(args[1], "buyer"))
    {
        actor = DEPTS + (int)number - 1;
    }
    else
    {
        return 1;
    }
    struct sigaction sa = { .sa_handler = SIG_IGN };
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    char token[32];
    snprintf(token, sizeof(token), "%ld", pid);
    struct Ipc ipc;
    int connected = ipc_connect(&ipc, mode, token, actor);
    int result = connected ? 1 : worker_loop(&ipc, actor);
    ipc_close(&ipc);
    return result;
}

int main(int argc, char **argv)
{
    if (argc == 2 && !strcmp(argv[1], "--help"))
    {
        dprintf(STDOUT_FILENO, "Запуск: ./shop MODE INPUT LOG DELAY_MS SEED\n"
            "MODE: fork-named | fork-unnamed | shm | mq\n"
            "В режимах shm и mq: ./actor MODE SESSION seller|buyer NUMBER\n"
            "SESSION равен PID процесса shop.\n");
        return 0;
    }
    enum Mode mode = argc > 1 ? mode_of(argv[1]) : 0;
#ifdef BUILD_WORKER
    if (argc != 5 || (mode != SHM && mode != MQ))
    {
        dprintf(STDERR_FILENO, "Запуск: ./actor shm|mq SESSION seller|buyer NUMBER\n");
        return 1;
    }
    return independent_actor(mode, argv + 2);
#else
    if (argc != 6 || !mode)
    {
        dprintf(STDERR_FILENO, "Запуск: ./shop MODE INPUT LOG DELAY_MS SEED\n");
        return 1;
    }
    return server(mode, argv + 2);
#endif
}
