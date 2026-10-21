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

/* ИДЗ-2: три продавца и N покупателей работают в отдельных POSIX-потоках.
 * Мьютекс защищает модель/журнал; семафоры доставляют задания потокам.
 * Главный поток продвигает логическое время только после подтверждения
 * обработки ВСЕХ сообщений текущего такта: реальное время не влияет на модель.
 */
#define DEPARTMENTS 3
#define MAX_BUYERS 100
#define MAX_TIME 100000
#define MAX_SERVICE 10000
#define INPUT_SIZE 65536
#define FOREVER INT64_MAX

enum BuyerState { NOT_ARRIVED, WAITING, SERVING, READY, LEFT, REJECTED };
struct Shop;

struct Buyer {
    struct Shop *shop;
    int id;
    int route[DEPARTMENTS], length, position;
    enum BuyerState state;
    sem_t gate;                 /* разрешение на вход ИЛИ уведомление о покупке */
    pthread_t thread;
};

struct Department {
    struct Shop *shop;
    int id, minimum, maximum;
    int queue[MAX_BUYERS], head, tail;
    int buyer;                 /* индекс обслуживаемого; -1 = свободен */
    int64_t finish_time;
    int purchases;
    unsigned int rng_state;    /* отдельное состояние ГСЧ для потока продавца */
    sem_t work;                /* количество заявок в очереди */
    sem_t finish;              /* разрешение завершить обслуживание в такте */
    pthread_t thread;
};

struct Shop {
    int count, interval, closing_time, delay_ms;
    unsigned int seed;
    struct Buyer buyers[MAX_BUYERS];
    struct Department departments[DEPARTMENTS];
    pthread_mutex_t mu;       /* ВСЕ поля модели ниже и выше защищены mu */
    pthread_cond_t changed;   /* ожидание подтверждений и прерываемой задержки */
    int64_t now;
    int pending;              /* отправленные, но НЕ обработанные сообщения */
    int closed, shutdown, stop_signal, io_error;
    int log_fd;
    atomic_int interrupt_requested; /* позволяет прервать забитый stdout */
};

struct SignalArgs {
    struct Shop *shop;
    sigset_t set;
};

/* Дескрипторный вывод; POLLOUT нужен при заполненном канале stdout. */
static int write_all(struct Shop *s, int fd, const char *buf, size_t size,
                     int interruptible)
{
    size_t used = 0;
    while (used < size) {
        if (interruptible && atomic_load(&s->interrupt_requested)) return 1;
        ssize_t n = write(fd, buf + used, size - used);
        if (n > 0) {
            used += (size_t)n;
        } else if (n < 0 && errno == EINTR) {
            continue;
        } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            struct pollfd p = {.fd = fd, .events = POLLOUT};
            int rc = poll(&p, 1, 50);
            if (rc < 0 && errno != EINTR) return -1;
        } else {
            if (n == 0) errno = EIO;
            return -1;
        }
    }
    return 0;
}

/* Вызывать только с захваченным mu: строки журнала не перемешиваются. */
static void event_locked(struct Shop *s, const char *fmt, ...)
{
    char message[1024], line[1120];
    va_list args;
    va_start(args, fmt);
    int n = vsnprintf(message, sizeof(message), fmt, args);
    va_end(args);
    if (n < 0 || (size_t)n >= sizeof(message)) {
        s->io_error = 1;
        return;
    }
    int m = snprintf(line, sizeof(line), "[t=%lld] %s\n",
                     (long long)s->now, message);
    if (m < 0 || (size_t)m >= sizeof(line)) {
        s->io_error = 1;
        return;
    }
    /* Пишем журнал даже при прерванном выводе в терминал. */
    int out = write_all(s, STDOUT_FILENO, line, (size_t)m, 1);
    if (out < 0) s->io_error = 1;
    if (write_all(s, s->log_fd, line, (size_t)m, 0) != 0) s->io_error = 1;
}

static int next_number(char **cursor, long min, long max, long *value)
{
    char *p = *cursor;
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') ++p;
    char *end;
    errno = 0;
    long n = strtol(p, &end, 10);
    if (end == p || errno == ERANGE || n < min || n > max) return -1;
    if (*end && *end != ' ' && *end != '\t' && *end != '\r' && *end != '\n') return -1;
    *cursor = end;
    *value = n;
    return 0;
}

static int arg_number(char *s, long max, long *out)
{
    return next_number(&s, 0, max, out) == 0 && *s == '\0' ? 0 : -1;
}

/* Формат совместим с hw1, включая проверки переполнений и дублей маршрута. */
static int read_input(const char *path, struct Shop *s, struct stat *input_stat)
{
    char data[INPUT_SIZE];
    size_t used = 0;
    int fd = open(path, O_RDONLY | O_NONBLOCK);
    if (fd < 0) { perror("Входной файл"); return -1; }
    if (fstat(fd, input_stat) != 0 || !S_ISREG(input_stat->st_mode)) {
        dprintf(STDERR_FILENO, "Вход должен быть обычным файлом\n");
        close(fd);
        return -1;
    }
    for (;;) {
        ssize_t n = read(fd, data + used, sizeof(data) - 1 - used);
        if (n < 0 && errno == EINTR) continue;
        if (n < 0) { perror("Чтение входа"); close(fd); return -1; }
        if (n == 0) break;
        used += (size_t)n;
        if (used == sizeof(data) - 1) {
            dprintf(STDERR_FILENO, "Входной файл слишком большой\n");
            close(fd);
            return -1;
        }
    }
    if (close(fd) != 0) { perror("Закрытие входа"); return -1; }
    if (memchr(data, '\0', used)) {
        dprintf(STDERR_FILENO, "Недопустимый нулевой байт\n");
        return -1;
    }
    data[used] = '\0';
    char *p = data;
    long value;
    const char *field = "N";
    int buyer_no = 0, department_no = 0, step_no = 0;
    if (next_number(&p, 0, MAX_BUYERS, &value)) goto invalid;
    s->count = (int)value;
    field = "INTERVAL";
    if (next_number(&p, 0, MAX_TIME, &value)) goto invalid;
    s->interval = (int)value;
    field = "CLOSE";
    if (next_number(&p, 0, MAX_TIME, &value)) goto invalid;
    s->closing_time = (int)value;
    for (int d = 0; d < DEPARTMENTS; d++) {
        struct Department *dep = &s->departments[d];
        department_no = d + 1;
        field = "MIN (1..10000)";
        if (next_number(&p, 1, MAX_SERVICE, &value)) goto invalid;
        dep->minimum = (int)value;
        field = "MAX (MIN..10000)";
        if (next_number(&p, dep->minimum, MAX_SERVICE, &value)) goto invalid;
        dep->maximum = (int)value;
        dep->buyer = -1;
    }
    department_no = 0;
    for (int i = 0; i < s->count; i++) {
        struct Buyer *b = &s->buyers[i];
        buyer_no = i + 1;
        step_no = 0;
        field = "K (1..3)";
        if (next_number(&p, 1, DEPARTMENTS, &value)) goto invalid;
        b->length = (int)value;
        for (int j = 0; j < b->length; j++) {
            step_no = j + 1;
            field = "номер отдела (1..3)";
            if (next_number(&p, 1, DEPARTMENTS, &value)) goto invalid;
            b->route[j] = (int)value - 1;
            for (int k = 0; k < j; k++) {
                if (b->route[k] == b->route[j]) {
                    field = "повтор отдела в маршруте";
                    goto invalid;
                }
            }
        }
    }
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') ++p;
    if (*p) {
        field = "лишние данные";
        buyer_no = step_no = 0;
        goto invalid;
    }
    return 0;
invalid:
    dprintf(STDERR_FILENO, "Некорректные данные: %s (покупатель=%d, шаг=%d, отдел=%d)\n",
            field, buyer_no, step_no, department_no);
    return -1;
}

/* Не допускает перезаписи входа через hardlink/symlink с тем же inode. */
static int open_log(const char *path, const struct stat *source)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_NONBLOCK, 0644);
    if (fd < 0) { perror("Журнал"); return -1; }
    struct stat target;
    if (fstat(fd, &target) != 0 || !S_ISREG(target.st_mode) ||
        (source->st_dev == target.st_dev && source->st_ino == target.st_ino)) {
        dprintf(STDERR_FILENO, "Журнал должен быть обычным файлом, отличным от входа\n");
        close(fd);
        return -1;
    }
    if (ftruncate(fd, 0) != 0) { perror("Очистка журнала"); close(fd); return -1; }
    return fd;
}

/* Число pending корректируется ДО каждого sem_post (всё под mu). */
static void send_locked(struct Shop *s, sem_t *gate)
{
    ++s->pending;
    if (sem_post(gate) != 0) s->io_error = 1;
}
static void ack_locked(struct Shop *s)
{
    --s->pending;
    if (s->pending == 0) pthread_cond_broadcast(&s->changed);
}

static void enqueue_locked(struct Shop *s, struct Buyer *b)
{
    int d = b->route[b->position];
    struct Department *dep = &s->departments[d];
    event_locked(s, "Переход: покупатель=%d отдел=%d", b->id + 1, d + 1);
    if (dep->buyer >= 0 || dep->head < dep->tail) {
        event_locked(s, "Очередь: покупатель=%d отдел=%d позиция=%d",
                     b->id + 1, d + 1, dep->tail - dep->head + 1);
    }
    /* В каждом отделе покупатель бывает максимум раз: вместимость <= N. */
    int should_assign = (dep->buyer < 0 && dep->head == dep->tail);
    dep->queue[dep->tail++] = b->id;
    b->state = WAITING;
    /* Очередь может расти, пока продавец занят: эти элементы НЕ являются
     * незавершёнными сообщениями текущего такта. */
    if (should_assign) ++s->pending;
    if (sem_post(&dep->work) != 0) s->io_error = 1;
}

static void *buyer_thread(void *arg)
{
    struct Buyer *b = arg;
    struct Shop *s = b->shop;
    for (;;) {
        while (sem_wait(&b->gate) == -1 && errno == EINTR) {}
        pthread_mutex_lock(&s->mu);
        if (s->shutdown) {
            pthread_mutex_unlock(&s->mu);
            break;
        }
        if (b->state == NOT_ARRIVED) {
            int64_t arrival = (int64_t)b->id * s->interval;
            if (s->closed || arrival >= s->closing_time) {
                b->state = REJECTED;
                event_locked(s, "Отказ: покупатель=%d причина=вход_закрыт плановое_прибытие=%lld",
                             b->id + 1, (long long)arrival);
            } else {
                char route[32] = "";
                size_t offset = 0;
                for (int j = 0; j < b->length; j++) {
                    int n = snprintf(route + offset, sizeof(route) - offset,
                                     "%s%d", j ? "->" : "", b->route[j] + 1);
                    if (n > 0) offset += (size_t)n;
                }
                event_locked(s, "Вход: покупатель=%d", b->id + 1);
                event_locked(s, "План: покупатель=%d отделы=%s", b->id + 1, route);
                enqueue_locked(s, b);
            }
        } else if (b->state == READY) {
            if (b->position == b->length) {
                b->state = LEFT;
                event_locked(s, "Выход: покупатель=%d план=выполнен", b->id + 1);
            } else {
                enqueue_locked(s, b);
            }
        } else {
            /* Такое сообщение означает ошибку протокола синхронизации. */
            s->io_error = 1;
        }
        int finished = (b->state == LEFT || b->state == REJECTED);
        ack_locked(s);
        pthread_mutex_unlock(&s->mu);
        if (finished) break;
    }
    return NULL;
}

static void *seller_thread(void *arg)
{
    struct Department *dep = arg;
    struct Shop *s = dep->shop;
    for (;;) {
        while (sem_wait(&dep->work) == -1 && errno == EINTR) {}
        pthread_mutex_lock(&s->mu);
        if (s->shutdown) {
            pthread_mutex_unlock(&s->mu);
            break;
        }
        if (dep->head == dep->tail || dep->buyer != -1) {
            s->io_error = 1;
            ack_locked(s);
            pthread_mutex_unlock(&s->mu);
            break;
        }
        int who = dep->queue[dep->head++];
        struct Buyer *b = &s->buyers[who];
        int duration = dep->minimum;
        if (dep->minimum < dep->maximum) {
            duration += (int)((unsigned)rand_r(&dep->rng_state) %
                              (unsigned)(dep->maximum - dep->minimum + 1));
        }
        dep->buyer = who;
        dep->finish_time = s->now + duration;
        b->state = SERVING;
        event_locked(s, "Начало: покупатель=%d отдел=%d окончание=%lld",
                     who + 1, dep->id + 1, (long long)dep->finish_time);
        ack_locked(s);
        pthread_mutex_unlock(&s->mu);

        while (sem_wait(&dep->finish) == -1 && errno == EINTR) {}
        pthread_mutex_lock(&s->mu);
        if (s->shutdown) {
            pthread_mutex_unlock(&s->mu);
            break;
        }
        /* В каждом отделе один продавец и ровно один обслуживаемый. */
        event_locked(s, "Конец: покупатель=%d отдел=%d", who + 1, dep->id + 1);
        ++dep->purchases;
        dep->buyer = -1;
        ++b->position;
        b->state = READY;
        send_locked(s, &b->gate);
        /* Следующий покупатель очереди может начать обслуживание сразу
         * в том же логическом такте, когда продавец освободился. */
        if (dep->head < dep->tail) ++s->pending;
        ack_locked(s);
        pthread_mutex_unlock(&s->mu);
    }
    return NULL;
}

/* Общее прерывание разблокирует ВСЕ ожидающие sem_wait потоки. */
static void shutdown_locked(struct Shop *s)
{
    if (s->shutdown) return;
    s->shutdown = 1;
    for (int i = 0; i < s->count; i++) sem_post(&s->buyers[i].gate);
    for (int d = 0; d < DEPARTMENTS; d++) {
        sem_post(&s->departments[d].work);
        sem_post(&s->departments[d].finish);
    }
    pthread_cond_broadcast(&s->changed);
}

/* sigwait безопаснее асинхронного обработчика: нет вызовов pthread из handler. */
static void *signal_thread(void *arg)
{
    struct SignalArgs *sa = arg;
    int number = 0;
    if (sigwait(&sa->set, &number) == 0 && number != SIGUSR1) {
        atomic_store(&sa->shop->interrupt_requested, 1);
        pthread_mutex_lock(&sa->shop->mu);
        if (!sa->shop->shutdown) {
            sa->shop->stop_signal = number;
            shutdown_locked(sa->shop);
        }
        pthread_mutex_unlock(&sa->shop->mu);
    }
    return NULL;
}

/* Пауза для наглядного вывода; время всё равно продвигает только scheduler. */
static void delay_locked(struct Shop *s, int64_t ticks)
{
    if (s->delay_ms == 0 || ticks == 0) return;
    struct timespec until;
    if (clock_gettime(CLOCK_MONOTONIC, &until) != 0) {
        s->io_error = 1;
        return;
    }
    int64_t nanosec = (int64_t)s->delay_ms * ticks * 1000000;
    until.tv_sec += (time_t)(nanosec / 1000000000);
    until.tv_nsec += (long)(nanosec % 1000000000);
    if (until.tv_nsec >= 1000000000L) { ++until.tv_sec; until.tv_nsec -= 1000000000L; }
    while (!s->shutdown && !s->io_error) {
        int rc = pthread_cond_timedwait(&s->changed, &s->mu, &until);
        if (rc == ETIMEDOUT) break;
        if (rc != 0) { s->io_error = 1; break; }
    }
}

/* Планировщик рассылает события; действие выполняет поток соответствующей
 * сущности. Барьер pending гарантирует отсутствие скачков логического времени
 * во время обработки покупателем/продавцом текущего события. */
static void simulate(struct Shop *s)
{
    pthread_mutex_lock(&s->mu);
    event_locked(s, "Запуск: отделов=3 продавцов_на_отдел=1 покупателей=%d интервал=%d закрытие=%d задержка_мс=%d seed=%u",
                 s->count, s->interval, s->closing_time, s->delay_ms, s->seed);
    for (int d = 0; d < DEPARTMENTS; d++) {
        struct Department *dep = &s->departments[d];
        event_locked(s, "Параметры: отдел=%d минимум=%d максимум=%d",
                     d + 1, dep->minimum, dep->maximum);
    }
    while (!s->shutdown && !s->io_error) {
        int64_t next = s->closed ? FOREVER : s->closing_time;
        if (!s->closed) {
            for (int i = 0; i < s->count; i++) {
                int64_t arrival = (int64_t)i * s->interval;
                if (s->buyers[i].state == NOT_ARRIVED && arrival < s->closing_time &&
                    arrival < next) next = arrival;
            }
        }
        for (int d = 0; d < DEPARTMENTS; d++) {
            struct Department *dep = &s->departments[d];
            if (dep->buyer >= 0 && dep->finish_time < next) next = dep->finish_time;
        }
        if (next == FOREVER) break; /* все покупатели вышли / получили отказ */
        delay_locked(s, next - s->now);
        if (s->shutdown || s->io_error) break;
        s->now = next;
        if (!s->closed && next == s->closing_time) {
            s->closed = 1;
            event_locked(s, "Вход закрыт");
            for (int i = 0; i < s->count; i++) {
                if (s->buyers[i].state == NOT_ARRIVED)
                    send_locked(s, &s->buyers[i].gate);
            }
        } else if (!s->closed) {
            for (int i = 0; i < s->count; i++) {
                if (s->buyers[i].state == NOT_ARRIVED &&
                    (int64_t)i * s->interval == next)
                    send_locked(s, &s->buyers[i].gate);
            }
        }
        for (int d = 0; d < DEPARTMENTS; d++) {
            struct Department *dep = &s->departments[d];
            if (dep->buyer >= 0 && dep->finish_time == next)
                send_locked(s, &dep->finish);
        }
        while (s->pending > 0 && !s->shutdown && !s->io_error)
            pthread_cond_wait(&s->changed, &s->mu);
    }
    if (s->io_error && !s->shutdown)
        dprintf(STDERR_FILENO, "Ошибка ввода-вывода / синхронизации\n");
    shutdown_locked(s);
    pthread_mutex_unlock(&s->mu);
}

static void summary_locked(struct Shop *s)
{
    int entered = 0, left = 0, rejected = 0, future = 0;
    for (int i = 0; i < s->count; i++) {
        switch (s->buyers[i].state) {
            case NOT_ARRIVED: ++future; break;
            case REJECTED: ++rejected; break;
            default:
                ++entered;
                if (s->buyers[i].state == LEFT) ++left;
        }
    }
    const char *reason = s->io_error ? "ошибка_ввода_вывода" :
                         s->stop_signal == SIGINT ? "SIGINT" :
                         s->stop_signal == SIGTERM ? "SIGTERM" : "завершено";
    event_locked(s, "Итог: причина=%s", reason);
    event_locked(s, "Покупатели: всего=%d вошли=%d вышли=%d внутри=%d не_допущены=%d ещё_не_пришли=%d",
                 s->count, entered, left, entered - left, rejected, future);
    for (int d = 0; d < DEPARTMENTS; d++) {
        struct Department *dep = &s->departments[d];
        event_locked(s, "Отдел: номер=%d покупок=%d очередь=%d обслуживается=%d",
                     d + 1, dep->purchases, dep->tail - dep->head, dep->buyer + 1);
    }
    for (int i = 0; i < s->count; i++) {
        struct Buyer *b = &s->buyers[i];
        if (b->state == WAITING || b->state == SERVING || b->state == READY) {
            event_locked(s, "Остался: покупатель=%d состояние=%s отдел=%d выполнено=%d из=%d",
                         i + 1, b->state == WAITING ? "очередь" :
                         b->state == SERVING ? "обслуживание" : "переход",
                         b->route[b->position < b->length ? b->position : b->length - 1] + 1,
                         b->position, b->length);
        }
    }
}

static int run(struct Shop *s)
{
    pthread_condattr_t ca;
    if (pthread_mutex_init(&s->mu, NULL) != 0) return 1;
    if (pthread_condattr_init(&ca) != 0) { pthread_mutex_destroy(&s->mu); return 1; }
    if (pthread_condattr_setclock(&ca, CLOCK_MONOTONIC) != 0) {
        pthread_condattr_destroy(&ca);
        pthread_mutex_destroy(&s->mu);
        return 1;
    }
    if (pthread_cond_init(&s->changed, &ca) != 0) {
        pthread_condattr_destroy(&ca);
        pthread_mutex_destroy(&s->mu);
        return 1;
    }
    pthread_condattr_destroy(&ca);
    int initialized_buyers = 0, initialized_deps = 0;
    int created_buyers = 0, created_deps = 0;
    int failed = 0, have_watcher = 0;
    pthread_t watcher;
    struct SignalArgs sigargs = {.shop = s};
    sigemptyset(&sigargs.set);
    sigaddset(&sigargs.set, SIGINT);
    sigaddset(&sigargs.set, SIGTERM);
    sigaddset(&sigargs.set, SIGUSR1); /* только для остановки sigwait */
    for (int i = 0; i < s->count; i++) {
        struct Buyer *b = &s->buyers[i];
        b->shop = s;
        b->id = i;
        if (sem_init(&b->gate, 0, 0) != 0) { failed = 1; goto cleanup; }
        ++initialized_buyers;
    }
    for (int d = 0; d < DEPARTMENTS; d++) {
        struct Department *dep = &s->departments[d];
        dep->shop = s;
        dep->id = d;
        dep->rng_state = s->seed ^ (0x9e3779b9U * (unsigned)(d + 1));
        if (sem_init(&dep->work, 0, 0) != 0) { failed = 1; goto cleanup; }
        if (sem_init(&dep->finish, 0, 0) != 0) {
            sem_destroy(&dep->work);
            failed = 1;
            goto cleanup;
        }
        ++initialized_deps;
    }
    int rc = pthread_create(&watcher, NULL, signal_thread, &sigargs);
    if (rc != 0) { errno = rc; perror("pthread_create(sigwait)"); failed = 1; goto cleanup; }
    have_watcher = 1;
    for (int d = 0; d < DEPARTMENTS; d++) {
        rc = pthread_create(&s->departments[d].thread, NULL, seller_thread,
                            &s->departments[d]);
        if (rc != 0) { errno = rc; perror("pthread_create(продавец)"); failed = 1; goto cleanup; }
        ++created_deps;
    }
    for (int i = 0; i < s->count; i++) {
        rc = pthread_create(&s->buyers[i].thread, NULL, buyer_thread, &s->buyers[i]);
        if (rc != 0) { errno = rc; perror("pthread_create(покупатель)"); failed = 1; goto cleanup; }
        ++created_buyers;
    }
    simulate(s);
cleanup:
    /* До полной инициализации семафоров рабочих потоков ещё нет. */
    if (initialized_buyers == s->count && initialized_deps == DEPARTMENTS) {
        pthread_mutex_lock(&s->mu);
        shutdown_locked(s);
        pthread_mutex_unlock(&s->mu);
    }
    for (int i = 0; i < created_buyers; i++) pthread_join(s->buyers[i].thread, NULL);
    for (int d = 0; d < created_deps; d++) pthread_join(s->departments[d].thread, NULL);
    if (have_watcher) {
        pthread_kill(watcher, SIGUSR1);
        pthread_join(watcher, NULL);
    }
    if (!failed) {
        pthread_mutex_lock(&s->mu);
        summary_locked(s);
        pthread_mutex_unlock(&s->mu);
    }
    int status = failed || s->io_error ? 1 :
                 s->stop_signal ? 128 + s->stop_signal : 0;
    for (int i = 0; i < initialized_buyers; i++) sem_destroy(&s->buyers[i].gate);
    for (int d = 0; d < initialized_deps; d++) {
        sem_destroy(&s->departments[d].work);
        sem_destroy(&s->departments[d].finish);
    }
    pthread_cond_destroy(&s->changed);
    pthread_mutex_destroy(&s->mu);
    return status;
}

int main(int argc, char **argv)
{
    if (argc == 2 && strcmp(argv[1], "--help") == 0) {
        dprintf(STDOUT_FILENO,
                "Запуск: %s INPUT_FILE LOG_FILE DELAY_MS SEED\n"
                "Файл: N INTERVAL CLOSE, три пары MIN MAX, N маршрутов K D1 ... DK\n"
                "N: 0..100; INTERVAL,CLOSE: 0..100000; MIN,MAX: 1..10000\n"
                "K: 1..3, отделы 1..3 без повторов; DELAY_MS: 0..1000\n"
                "Покупатель i приходит в t=(i-1)*INTERVAL; при t>=CLOSE вход закрыт.\n"
                "DELAY_MS=0: максимально быстрое виртуальное время.\n", argv[0]);
        return 0;
    }
    if (argc != 5) {
        dprintf(STDERR_FILENO, "Запуск: %s INPUT_FILE LOG_FILE DELAY_MS SEED\n", argv[0]);
        return 1;
    }
    long delay, seed;
    if (arg_number(argv[3], 1000, &delay) || arg_number(argv[4], INT_MAX, &seed)) {
        dprintf(STDERR_FILENO, "DELAY_MS: 0..1000; SEED: 0..INT_MAX\n");
        return 1;
    }
    struct Shop *s = calloc(1, sizeof(*s));
    if (!s) { perror("calloc"); return 1; }
    s->log_fd = -1;
    s->delay_ms = (int)delay;
    s->seed = (unsigned int)seed;
    struct stat input_stat;
    if (read_input(argv[1], s, &input_stat) != 0) { free(s); return 1; }
    s->log_fd = open_log(argv[2], &input_stat);
    if (s->log_fd < 0) { free(s); return 1; }

    /* Все потоки наследуют заблокированные сигналы. Их ловит только sigwait. */
    sigset_t blocked, previous;
    sigemptyset(&blocked);
    sigaddset(&blocked, SIGINT);
    sigaddset(&blocked, SIGTERM);
    sigaddset(&blocked, SIGUSR1);
    if (pthread_sigmask(SIG_BLOCK, &blocked, &previous) != 0) {
        dprintf(STDERR_FILENO, "Ошибка блокировки сигналов\n");
        close(s->log_fd);
        free(s);
        return 1;
    }
    struct sigaction ignore = {.sa_handler = SIG_IGN};
    sigemptyset(&ignore.sa_mask);
    sigaction(SIGPIPE, &ignore, NULL);
    int original_flags = -1;
    struct stat stdstat;
    if (fstat(STDOUT_FILENO, &stdstat) == 0 && S_ISFIFO(stdstat.st_mode)) {
        original_flags = fcntl(STDOUT_FILENO, F_GETFL);
        if (original_flags >= 0 && fcntl(STDOUT_FILENO, F_SETFL,
                                         original_flags | O_NONBLOCK) != 0) {
            original_flags = -1;
        }
    }
    int status = run(s);
    if (original_flags >= 0) fcntl(STDOUT_FILENO, F_SETFL, original_flags);
    pthread_sigmask(SIG_SETMASK, &previous, NULL);
    if (close(s->log_fd) != 0) { perror("close(log)"); status = 1; }
    free(s);
    return status;
}
