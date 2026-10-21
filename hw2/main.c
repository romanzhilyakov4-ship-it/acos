#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define DEPTS 3
#define MAX_BUYERS 100
#define INPUT_SIZE 65536
#define INF INT64_MAX

enum State { NOT_ARRIVED, WAITING, SERVING, READY, LEFT, REJECTED };
struct Shop;

// Покупатель хранит маршрут и ожидает уведомления от планировщика или продавца
struct Buyer
{
    struct Shop *shop;
    int id, route[DEPTS], length, position;
    enum State state;
    sem_t gate;
    pthread_t thread;
};

// Продавец владеет очередью своего отдела и двумя семафорами
struct Dept
{
    struct Shop *shop;
    int id, min, max, queue[MAX_BUYERS], head, tail, buyer, purchases;
    int64_t finish_time;
    unsigned int random_state;
    sem_t work, finish;
    pthread_t thread;
};

// Общие данные модели защищены одним мьютексом
struct Shop
{
    int count, interval, closing, delay, closed, stop, shutdown, error, pending;
    int log_fd;
    int64_t now;
    unsigned int seed;
    atomic_int interrupted;
    pthread_mutex_t mutex;
    pthread_cond_t changed;
    struct Buyer buyers[MAX_BUYERS];
    struct Dept depts[DEPTS];
};

// Вывод поддерживает частичные записи и заполненный канал
static int write_all(struct Shop *s, int fd, const char *str, size_t len, int interruptible)
{
    for (size_t i = 0; i < len;)
    {
        if (interruptible && atomic_load(&s->interrupted))
        {
            return 0;
        }
        ssize_t n = write(fd, str + i, len - i);
        if (n > 0)
        {
            i += (size_t)n;
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

// Записывает событие в терминал и журнал, пока захвачен мьютекс
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
    n = snprintf(line, sizeof(line), "[t=%lld] %s\n", (long long)s->now, message);
    if (n < 0 || (size_t)n >= sizeof(line))
    {
        s->error = 1;
        return;
    }
    int output = write_all(s, STDOUT_FILENO, line, (size_t)n, 1);
    int saved = write_all(s, s->log_fd, line, (size_t)n, 0);
    if (output < 0 || saved < 0)
    {
        s->error = 1;
    }
}

// Строго разбирает очередное целое число
static int number(char **p, long min, long max, long *out)
{
    while (**p == ' ' || **p == '\n' || **p == '\t' || **p == '\r')
    {
        ++*p;
    }
    errno = 0;
    char *end;
    long n = strtol(*p, &end, 10);
    if (end == *p || errno == ERANGE || n < min || n > max ||
        (*end && *end != ' ' && *end != '\n' && *end != '\t' && *end != '\r'))
    {
        return -1;
    }
    *p = end;
    *out = n;
    return 0;
}

// Считывает параметры и маршруты, сохраняя формат ИДЗ-1
static int load(const char *path, struct Shop *s, struct stat *info)
{
    char buf[INPUT_SIZE];
    size_t used = 0;
    int fd = open(path, O_RDONLY | O_NONBLOCK);
    if (fd < 0)
    {
        perror("Входной файл");
        return -1;
    }
    if (fstat(fd, info) < 0 || !S_ISREG(info->st_mode))
    {
        dprintf(STDERR_FILENO, "Вход должен быть обычным файлом\n");
        close(fd);
        return -1;
    }
    while (used < sizeof(buf) - 1)
    {
        ssize_t n = read(fd, buf + used, sizeof(buf) - 1 - used);
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
    int too_large = used == sizeof(buf) - 1;
    if (close(fd) < 0 || too_large || memchr(buf, 0, used))
    {
        dprintf(STDERR_FILENO, "Некорректный входной файл\n");
        return -1;
    }
    buf[used] = 0;
    char *p = buf;
    long x;
    if (number(&p, 0, MAX_BUYERS, &x))
    {
        goto invalid;
    }
    s->count = (int)x;
    if (number(&p, 0, 100000, &x))
    {
        goto invalid;
    }
    s->interval = (int)x;
    if (number(&p, 0, 100000, &x))
    {
        goto invalid;
    }
    s->closing = (int)x;
    for (int d = 0; d < DEPTS; d++)
    {
        struct Dept *dep = &s->depts[d];
        if (number(&p, 1, 10000, &x))
        {
            goto invalid;
        }
        dep->min = (int)x;
        if (number(&p, dep->min, 10000, &x))
        {
            goto invalid;
        }
        dep->max = (int)x;
        dep->buyer = -1;
    }
    for (int i = 0; i < s->count; i++)
    {
        struct Buyer *b = &s->buyers[i];
        if (number(&p, 1, DEPTS, &x))
        {
            goto invalid;
        }
        b->length = (int)x;
        for (int j = 0; j < b->length; j++)
        {
            if (number(&p, 1, DEPTS, &x))
            {
                goto invalid;
            }
            b->route[j] = (int)x - 1;
            for (int k = 0; k < j; k++)
            {
                if (b->route[k] == b->route[j])
                {
                    goto invalid;
                }
            }
        }
    }
    while (*p == ' ' || *p == '\n' || *p == '\t' || *p == '\r')
    {
        p++;
    }
    if (*p)
    {
        goto invalid;
    }
    return 0;

invalid:
    dprintf(STDERR_FILENO, "Некорректные данные во входном файле\n");
    return -1;
}

// Запрещает использовать входной файл в качестве журнала
static int open_log(const char *path, const struct stat *input)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_NONBLOCK, 0644);
    struct stat info;
    if (fd < 0)
    {
        return -1;
    }
    if (fstat(fd, &info) < 0 || !S_ISREG(info.st_mode) ||
        (info.st_dev == input->st_dev && info.st_ino == input->st_ino) || ftruncate(fd, 0) < 0)
    {
        close(fd);
        return -1;
    }
    return fd;
}

// Каждое уведомление учитывается планировщиком до его обработки
static void send(struct Shop *s, sem_t *sem)
{
    ++s->pending;
    if (sem_post(sem))
    {
        s->error = 1;
    }
}

static void ack(struct Shop *s)
{
    if (--s->pending == 0)
    {
        pthread_cond_broadcast(&s->changed);
    }
}

// Покупатель посещает каждый отдел не более одного раза
static void enqueue(struct Shop *s, struct Buyer *b)
{
    struct Dept *d = &s->depts[b->route[b->position]];
    event(s, "Переход: покупатель=%d отдел=%d", b->id + 1, d->id + 1);
    if (d->buyer >= 0 || d->head < d->tail)
    {
        event(s, "Очередь: покупатель=%d отдел=%d позиция=%d",
            b->id + 1, d->id + 1, d->tail - d->head + 1);
    }
    if (d->buyer < 0 && d->head == d->tail)
    {
        ++s->pending;
    }
    d->queue[d->tail++] = b->id;
    b->state = WAITING;
    if (sem_post(&d->work))
    {
        s->error = 1;
    }
}

// Поток покупателя ожидает входа и завершения каждой покупки
static void *buyer_thread(void *arg)
{
    struct Buyer *b = arg;
    struct Shop *s = b->shop;
    for (;;)
    {
        while (sem_wait(&b->gate) && errno == EINTR)
        {
        }
        pthread_mutex_lock(&s->mutex);
        if (s->shutdown)
        {
            pthread_mutex_unlock(&s->mutex);
            break;
        }
        if (b->state == NOT_ARRIVED)
        {
            if (s->closed)
            {
                b->state = REJECTED;
                event(s, "Отказ: покупатель=%d причина=вход_закрыт плановое_прибытие=%lld",
                    b->id + 1, (long long)((int64_t)b->id * s->interval));
            }
            else
            {
                char route[16];
                int pos = 0;
                for (int j = 0; j < b->length; j++)
                {
                    pos += snprintf(route + pos, sizeof(route) - (size_t)pos,
                        "%s%d", j ? "->" : "", b->route[j] + 1);
                }
                event(s, "Вход: покупатель=%d", b->id + 1);
                event(s, "План: покупатель=%d отделы=%s", b->id + 1, route);
                enqueue(s, b);
            }
        }
        else if (b->state == READY)
        {
            if (b->position == b->length)
            {
                b->state = LEFT;
                event(s, "Выход: покупатель=%d план=выполнен", b->id + 1);
            }
            else
            {
                enqueue(s, b);
            }
        }
        else
        {
            s->error = 1;
        }
        int finished = b->state == LEFT || b->state == REJECTED;
        ack(s);
        pthread_mutex_unlock(&s->mutex);
        if (finished)
        {
            break;
        }
    }
    return NULL;
}

// Продавец начинает и завершает обслуживание по уведомлениям
static void *seller_thread(void *arg)
{
    struct Dept *d = arg;
    struct Shop *s = d->shop;
    for (;;)
    {
        while (sem_wait(&d->work) && errno == EINTR)
        {
        }
        pthread_mutex_lock(&s->mutex);
        if (s->shutdown)
        {
            pthread_mutex_unlock(&s->mutex);
            break;
        }
        if (d->head == d->tail || d->buyer != -1)
        {
            s->error = 1;
            ack(s);
            pthread_mutex_unlock(&s->mutex);
            break;
        }
        int id = d->queue[d->head++];
        struct Buyer *b = &s->buyers[id];
        int duration = d->min;
        if (d->min != d->max)
        {
            duration += (int)((unsigned)rand_r(&d->random_state) %
                (unsigned)(d->max - d->min + 1));
        }
        d->buyer = id;
        d->finish_time = s->now + duration;
        b->state = SERVING;
        event(s, "Начало: покупатель=%d отдел=%d окончание=%lld",
            id + 1, d->id + 1, (long long)d->finish_time);
        ack(s);
        pthread_mutex_unlock(&s->mutex);
        while (sem_wait(&d->finish) && errno == EINTR)
        {
        }
        pthread_mutex_lock(&s->mutex);
        if (s->shutdown)
        {
            pthread_mutex_unlock(&s->mutex);
            break;
        }
        event(s, "Конец: покупатель=%d отдел=%d", id + 1, d->id + 1);
        ++d->purchases;
        d->buyer = -1;
        ++b->position;
        b->state = READY;
        send(s, &b->gate);
        if (d->head < d->tail)
        {
            ++s->pending;
        }
        ack(s);
        pthread_mutex_unlock(&s->mutex);
    }
    return NULL;
}

// Остановка разблокирует все ожидающие потоки
static void shutdown_shop(struct Shop *s)
{
    if (s->shutdown)
    {
        return;
    }
    s->shutdown = 1;
    for (int i = 0; i < s->count; i++)
    {
        sem_post(&s->buyers[i].gate);
    }
    for (int d = 0; d < DEPTS; d++)
    {
        sem_post(&s->depts[d].work);
        sem_post(&s->depts[d].finish);
    }
    pthread_cond_broadcast(&s->changed);
}

// Сигналы безопасно принимаются отдельным потоком
static void *signal_thread(void *arg)
{
    struct Shop *s = arg;
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    sigaddset(&set, SIGUSR1);
    int sig = 0;
    if (!sigwait(&set, &sig) && sig != SIGUSR1)
    {
        atomic_store(&s->interrupted, 1);
        pthread_mutex_lock(&s->mutex);
        s->stop = sig;
        shutdown_shop(s);
        pthread_mutex_unlock(&s->mutex);
    }
    return NULL;
}

// Реальная задержка не влияет на модельное время
static void wait_delay(struct Shop *s, int64_t ticks)
{
    if (!ticks || !s->delay)
    {
        return;
    }
    struct timespec until;
    if (clock_gettime(CLOCK_MONOTONIC, &until))
    {
        s->error = 1;
        return;
    }
    int64_t ns = ticks * s->delay * 1000000LL;
    until.tv_sec += (time_t)(ns / 1000000000LL);
    until.tv_nsec += (long)(ns % 1000000000LL);
    if (until.tv_nsec >= 1000000000L)
    {
        until.tv_nsec -= 1000000000L;
        ++until.tv_sec;
    }
    while (!s->shutdown && !s->error)
    {
        int rc = pthread_cond_timedwait(&s->changed, &s->mutex, &until);
        if (rc == ETIMEDOUT)
        {
            break;
        }
        if (rc)
        {
            s->error = 1;
            break;
        }
    }
}

// Планировщик управляет логическим временем и барьером pending
static void simulate(struct Shop *s)
{
    pthread_mutex_lock(&s->mutex);
    event(s, "Запуск: отделов=3 продавцов_на_отдел=1 покупателей=%d интервал=%d закрытие=%d задержка_мс=%d seed=%u",
        s->count, s->interval, s->closing, s->delay, s->seed);
    for (int d = 0; d < DEPTS; d++)
    {
        event(s, "Параметры: отдел=%d минимум=%d максимум=%d",
            d + 1, s->depts[d].min, s->depts[d].max);
    }
    while (!s->shutdown && !s->error)
    {
        int64_t next = s->closed ? INF : s->closing;
        if (!s->closed)
        {
            for (int i = 0; i < s->count; i++)
            {
                int64_t arrival = (int64_t)i * s->interval;
                if (s->buyers[i].state == NOT_ARRIVED && arrival < next)
                {
                    next = arrival;
                }
            }
        }
        for (int d = 0; d < DEPTS; d++)
        {
            struct Dept *dep = &s->depts[d];
            if (dep->buyer >= 0 && dep->finish_time < next)
            {
                next = dep->finish_time;
            }
        }
        if (next == INF)
        {
            break;
        }
        wait_delay(s, next - s->now);
        if (s->shutdown || s->error)
        {
            break;
        }
        s->now = next;
        if (!s->closed && next == s->closing)
        {
            s->closed = 1;
            event(s, "Вход закрыт");
        }
        for (int i = 0; i < s->count; i++)
        {
            int64_t arrival = (int64_t)i * s->interval;
            if (s->buyers[i].state == NOT_ARRIVED &&
                (s->closed || arrival == next))
            {
                send(s, &s->buyers[i].gate);
            }
        }
        for (int d = 0; d < DEPTS; d++)
        {
            struct Dept *dep = &s->depts[d];
            if (dep->buyer >= 0 && dep->finish_time == next)
            {
                send(s, &dep->finish);
            }
        }
        while (s->pending > 0 && !s->shutdown && !s->error)
        {
            pthread_cond_wait(&s->changed, &s->mutex);
        }
    }
    shutdown_shop(s);
    pthread_mutex_unlock(&s->mutex);
}

// Печатает итог по всем покупателям и отделам
static void summary(struct Shop *s)
{
    int entered = 0, left = 0, rejected = 0, future = 0;
    for (int i = 0; i < s->count; i++)
    {
        enum State st = s->buyers[i].state;
        if (st == NOT_ARRIVED)
        {
            ++future;
        }
        else if (st == REJECTED)
        {
            ++rejected;
        }
        else
        {
            ++entered;
            if (st == LEFT)
            {
                ++left;
            }
        }
    }
    const char *reason = s->error ? "ошибка_ввода_вывода" :
        s->stop == SIGINT ? "SIGINT" : s->stop == SIGTERM ? "SIGTERM" : "завершено";
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
        if (b->state == WAITING || b->state == SERVING || b->state == READY)
        {
            event(s, "Остался: покупатель=%d состояние=%s отдел=%d выполнено=%d из=%d",
                i + 1, b->state == WAITING ? "очередь" :
                b->state == SERVING ? "обслуживание" : "переход",
                b->route[b->position < b->length ? b->position : b->length - 1] + 1,
                b->position, b->length);
        }
    }
}

// Создаёт потоки и освобождает все синхропримитивы
static int run(struct Shop *s)
{
    pthread_condattr_t attr;
    if (pthread_mutex_init(&s->mutex, NULL))
    {
        return 1;
    }
    if (pthread_condattr_init(&attr))
    {
        pthread_mutex_destroy(&s->mutex);
        return 1;
    }
    if (pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) ||
        pthread_cond_init(&s->changed, &attr))
    {
        pthread_condattr_destroy(&attr);
        pthread_mutex_destroy(&s->mutex);
        return 1;
    }
    pthread_condattr_destroy(&attr);
    int ib = 0, id = 0, cb = 0, cd = 0, failed = 0, watch_started = 0;
    pthread_t watcher;
    for (int i = 0; i < s->count; i++)
    {
        struct Buyer *b = &s->buyers[i];
        b->shop = s;
        b->id = i;
        if (sem_init(&b->gate, 0, 0))
        {
            failed = 1;
            goto cleanup;
        }
        ++ib;
    }
    for (int d = 0; d < DEPTS; d++)
    {
        struct Dept *dep = &s->depts[d];
        dep->shop = s;
        dep->id = d;
        dep->random_state = s->seed ^ (0x9e3779b9U * (unsigned)(d + 1));
        if (sem_init(&dep->work, 0, 0))
        {
            failed = 1;
            goto cleanup;
        }
        if (sem_init(&dep->finish, 0, 0))
        {
            sem_destroy(&dep->work);
            failed = 1;
            goto cleanup;
        }
        ++id;
    }
    int rc = pthread_create(&watcher, NULL, signal_thread, s);
    if (rc)
    {
        failed = 1;
        goto cleanup;
    }
    watch_started = 1;
    for (int d = 0; d < DEPTS; d++)
    {
        if (pthread_create(&s->depts[d].thread, NULL, seller_thread, &s->depts[d]))
        {
            failed = 1;
            goto cleanup;
        }
        ++cd;
    }
    for (int i = 0; i < s->count; i++)
    {
        if (pthread_create(&s->buyers[i].thread, NULL, buyer_thread, &s->buyers[i]))
        {
            failed = 1;
            goto cleanup;
        }
        ++cb;
    }
    simulate(s);

cleanup:
    if (ib == s->count && id == DEPTS)
    {
        pthread_mutex_lock(&s->mutex);
        shutdown_shop(s);
        pthread_mutex_unlock(&s->mutex);
    }
    for (int i = 0; i < cb; i++)
    {
        pthread_join(s->buyers[i].thread, NULL);
    }
    for (int d = 0; d < cd; d++)
    {
        pthread_join(s->depts[d].thread, NULL);
    }
    if (watch_started)
    {
        pthread_kill(watcher, SIGUSR1);
        pthread_join(watcher, NULL);
    }
    if (!failed)
    {
        summary(s);
    }
    int status = failed || s->error ? 1 : s->stop ? 128 + s->stop : 0;
    for (int i = 0; i < ib; i++)
    {
        sem_destroy(&s->buyers[i].gate);
    }
    for (int d = 0; d < id; d++)
    {
        sem_destroy(&s->depts[d].work);
        sem_destroy(&s->depts[d].finish);
    }
    pthread_cond_destroy(&s->changed);
    pthread_mutex_destroy(&s->mutex);
    return status;
}

// Проверяет аргументы, сигналы и файловые дескрипторы
int main(int argc, char **argv)
{
    if (argc == 2 && !strcmp(argv[1], "--help"))
    {
        return dprintf(STDOUT_FILENO,
            "Запуск: %s INPUT_FILE LOG_FILE DELAY_MS SEED\n"
            "Файл: N INTERVAL CLOSE; 3 пары MIN MAX; N маршрутов K D1 ... DK\n",
            argv[0]) < 0;
    }
    if (argc != 5)
    {
        dprintf(STDERR_FILENO, "Запуск: %s INPUT_FILE LOG_FILE DELAY_MS SEED\n", argv[0]);
        return 1;
    }
    long delay, seed;
    char *p = argv[3];
    if (number(&p, 0, 1000, &delay) || *p)
    {
        return 1;
    }
    p = argv[4];
    if (number(&p, 0, INT_MAX, &seed) || *p)
    {
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
    if (load(argv[1], s, &input))
    {
        free(s);
        return 1;
    }
    s->log_fd = open_log(argv[2], &input);
    if (s->log_fd < 0)
    {
        perror("Журнал");
        free(s);
        return 1;
    }
    sigset_t blocked, previous;
    sigemptyset(&blocked);
    sigaddset(&blocked, SIGINT);
    sigaddset(&blocked, SIGTERM);
    sigaddset(&blocked, SIGUSR1);
    if (pthread_sigmask(SIG_BLOCK, &blocked, &previous))
    {
        close(s->log_fd);
        free(s);
        return 1;
    }
    struct sigaction ignore = { .sa_handler = SIG_IGN };
    sigemptyset(&ignore.sa_mask);
    sigaction(SIGPIPE, &ignore, NULL);
    struct stat out;
    int flags = -1;
    if (!fstat(STDOUT_FILENO, &out) && S_ISFIFO(out.st_mode))
    {
        flags = fcntl(STDOUT_FILENO, F_GETFL);
        if (flags >= 0 && fcntl(STDOUT_FILENO, F_SETFL, flags | O_NONBLOCK))
        {
            flags = -1;
        }
    }
    int status = run(s);
    if (flags >= 0)
    {
        fcntl(STDOUT_FILENO, F_SETFL, flags);
    }
    pthread_sigmask(SIG_SETMASK, &previous, NULL);
    if (close(s->log_fd))
    {
        status = 1;
    }
    free(s);
    return status;
}
