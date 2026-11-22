#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define DEPTS 3
#define BUYERS 100
#define PEERS 180
#define LINE 320
#define OUTPUT 8192
#define NEVER INT64_MAX

enum BuyerState
{
    FUTURE,
    WAITING,
    SERVING,
    READY,
    LEFT,
    REJECTED
};

enum PeerRole
{
    UNKNOWN,
    BUYER,
    SELLER,
    WATCHER
};

// Покупатели и отделы хранят те же данные, что и в предыдущих заданиях
struct Buyer
{
    int route[DEPTS];
    int length;
    int position;
    enum BuyerState state;
};

struct Dept
{
    int min;
    int max;
    int queue[BUYERS];
    int head;
    int tail;
    int buyer;
    int purchases;
    int64_t finish;
};

struct Shop
{
    int count;
    int interval;
    int closing;
    int delay;
    int closed;
    int trace;
    int error;
    int log_fd;
    unsigned int seed;
    int64_t now;
    struct Buyer buyers[BUYERS];
    struct Dept depts[DEPTS];
};

// Для каждого подключения отдельно хранятся входящие и исходящие данные
struct Peer
{
    int fd;
    enum PeerRole role;
    int number;
    char input[512];
    size_t input_len;
    char output[OUTPUT];
    size_t output_len;
    int replay_pos;
};

// Ожидающий запрос можно снова отправить после переподключения клиента
struct Task
{
    int actor;
    int done;
    int result;
    unsigned int id;
    char text[128];
};

struct Server
{
    struct Shop shop;
    int listener;
    struct Peer peers[PEERS];
    struct Task task;
    char (*history)[LINE];
    int history_count;
    int history_capacity;
    unsigned int next_id;
};

static volatile sig_atomic_t interrupted;

static void on_signal(int number)
{
    interrupted = number;
}

static int read_number(char **text, long min, long max, long *value)
{
    while (**text == ' ' || **text == '\t' || **text == '\r' || **text == '\n')
    {
        ++*text;
    }
    errno = 0;
    char *end;
    long number = strtol(*text, &end, 10);
    if (end == *text || errno == ERANGE || number < min || number > max ||
        (*end && *end != ' ' && *end != '\t' && *end != '\r' && *end != '\n'))
    {
        return -1;
    }
    *text = end;
    *value = number;
    return 0;
}

// Читает число в поле модели после проверки диапазона
static int read_int(char **text, int min, int max, int *value)
{
    long number;
    if (read_number(text, min, max, &number))
    {
        return -1;
    }
    *value = (int)number;
    return 0;
}

static int argument(const char *text, long min, long max, long *value)
{
    char *cursor = (char *)text;
    return read_number(&cursor, min, max, value) == 0 && *cursor == '\0' ? 0 : -1;
}

// Проверяем входной файл до запуска сервера и создания журнала
static int load_shop(const char *path, struct Shop *shop, struct stat *source)
{
    char buffer[65536];
    int fd = open(path, O_RDONLY | O_NONBLOCK);
    if (fd < 0)
    {
        perror("Входной файл");
        return -1;
    }
    if (fstat(fd, source) != 0 || !S_ISREG(source->st_mode))
    {
        dprintf(STDERR_FILENO, "Вход должен быть обычным файлом\n");
        close(fd);
        return -1;
    }
    size_t used = 0;
    while (used < sizeof(buffer) - 1)
    {
        ssize_t count = read(fd, buffer + used, sizeof(buffer) - 1 - used);
        if (count < 0 && errno == EINTR)
        {
            continue;
        }
        if (count < 0)
        {
            perror("Чтение входного файла");
            close(fd);
            return -1;
        }
        if (count == 0)
        {
            break;
        }
        used += (size_t)count;
    }
    if (close(fd) != 0 || used == sizeof(buffer) - 1 || memchr(buffer, '\0', used))
    {
        dprintf(STDERR_FILENO, "Некорректный размер или содержимое файла\n");
        return -1;
    }
    buffer[used] = '\0';
    char *text = buffer;
    if (read_int(&text, 0, BUYERS, &shop->count) ||
        read_int(&text, 0, 100000, &shop->interval) ||
        read_int(&text, 0, 100000, &shop->closing))
    {
        goto invalid;
    }
    for (int d = 0; d < DEPTS; d++)
    {
        struct Dept *dep = &shop->depts[d];
        if (read_int(&text, 1, 10000, &dep->min) ||
            read_int(&text, dep->min, 10000, &dep->max))
        {
            goto invalid;
        }
        dep->buyer = -1;
    }
    for (int i = 0; i < shop->count; i++)
    {
        struct Buyer *buyer = &shop->buyers[i];
        if (read_int(&text, 1, DEPTS, &buyer->length))
        {
            goto invalid;
        }
        for (int j = 0; j < buyer->length; j++)
        {
            if (read_int(&text, 1, DEPTS, &buyer->route[j]))
            {
                goto invalid;
            }
            buyer->route[j]--;
            for (int k = 0; k < j; k++)
            {
                if (buyer->route[k] == buyer->route[j])
                {
                    goto invalid;
                }
            }
        }
    }
    while (*text == ' ' || *text == '\t' || *text == '\r' || *text == '\n')
    {
        text++;
    }
    if (*text != '\0')
    {
        goto invalid;
    }
    return 0;

invalid:
    dprintf(STDERR_FILENO, "Некорректные данные во входном файле\n");
    return -1;
}

// Журнал нельзя открывать поверх исходного файла, в том числе через ссылку
static int open_log(const char *path, const struct stat *source)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_NONBLOCK, 0644);
    if (fd < 0)
    {
        perror("Журнал");
        return -1;
    }
    struct stat target;
    if (fstat(fd, &target) != 0 || !S_ISREG(target.st_mode) ||
        (target.st_dev == source->st_dev && target.st_ino == source->st_ino) ||
        ftruncate(fd, 0) != 0)
    {
        dprintf(STDERR_FILENO, "Журнал должен быть обычным файлом, отличным от входа\n");
        close(fd);
        return -1;
    }
    return fd;
}

static int nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL);
    return flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0 ? -1 : 0;
}

static int write_all(int fd, const char *text, size_t length)
{
    size_t used = 0;
    while (used < length)
    {
        if (fd == STDOUT_FILENO && interrupted)
        {
            return 0;
        }
        ssize_t n = write(fd, text + used, length - used);
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

static void drop_peer(struct Peer *peer)
{
    if (peer->fd >= 0)
    {
        close(peer->fd);
    }
    peer->fd = -1;
    peer->role = UNKNOWN;
    peer->input_len = 0;
    peer->output_len = 0;
}

// У медленного клиента очередь вывода ограничена, но сервер продолжает работать
static int queue_text(struct Peer *peer, const char *text)
{
    size_t size = strlen(text);
    if (size > sizeof(peer->output) - peer->output_len)
    {
        drop_peer(peer);
        return -1;
    }
    memcpy(peer->output + peer->output_len, text, size);
    peer->output_len += size;
    return 0;
}

// Записываем событие в историю и журнал
static void event(struct Server *server, const char *format, ...)
{
    char line[LINE];
    int prefix = snprintf(line, sizeof(line), "[t=%" PRId64 "] ", server->shop.now);
    va_list args;
    va_start(args, format);
    int n = vsnprintf(line + prefix, sizeof(line) - (size_t)prefix, format, args);
    va_end(args);
    if (n < 0 || (size_t)n >= sizeof(line) - (size_t)prefix - 1)
    {
        server->shop.error = 1;
        return;
    }
    size_t length = (size_t)(prefix + n);
    line[length++] = '\n';
    line[length] = '\0';
    if (server->history_count == server->history_capacity)
    {
        if (server->history_capacity > INT_MAX / 2)
        {
            server->shop.error = 1;
            return;
        }
        int capacity = server->history_capacity ? server->history_capacity * 2 : 256;
        void *history = realloc(server->history, (size_t)capacity * LINE);
        if (!history)
        {
            server->shop.error = 1;
            return;
        }
        server->history = history;
        server->history_capacity = capacity;
    }
    memcpy(server->history[server->history_count++], line, length + 1);
    int logged = write_all(server->shop.log_fd, line, length);
    if (logged < 0 || (server->shop.trace && write_all(STDOUT_FILENO, line, length) < 0))
    {
        server->shop.error = 1;
    }
}

// Наблюдатель получает историю по частям, затем новые события
static void prepare_replay(struct Server *server, struct Peer *peer)
{
    if (peer->role != WATCHER || peer->replay_pos < 0)
    {
        return;
    }
    while (peer->replay_pos < server->history_count &&
        peer->output_len < OUTPUT - LINE - 20)
    {
        char line[LINE + 16];
        snprintf(line, sizeof(line), "EVENT %s", server->history[peer->replay_pos]);
        if (queue_text(peer, line) != 0)
        {
            return;
        }
        peer->replay_pos++;
    }
}

static int actor_id(const struct Peer *peer)
{
    return peer->role == SELLER ? peer->number :
        peer->role == BUYER ? DEPTS + peer->number : -1;
}

// Находим действующее соединение участника по его номеру
static struct Peer *find_actor(struct Server *server, int actor)
{
    for (int i = 0; i < PEERS; i++)
    {
        struct Peer *peer = &server->peers[i];
        if (peer->fd >= 0 && actor_id(peer) == actor)
        {
            return peer;
        }
    }
    return NULL;
}

static void notify_actor(struct Server *server, int actor, const char *message)
{
    struct Peer *peer = find_actor(server, actor);
    if (peer)
    {
        queue_text(peer, message);
    }
}

// При повторном подключении участник получает актуальное состояние
static void identify(struct Server *server, struct Peer *peer, const char *line)
{
    if (!strcmp(line, "WATCH"))
    {
        peer->role = WATCHER;
        peer->replay_pos = 0;
        queue_text(peer, "WELCOME WATCH\n");
        return;
    }
    char role[16];
    int offset = 0;
    int number;
    if (sscanf(line, "HELLO %15s %n", role, &offset) != 1 || !offset)
    {
        queue_text(peer, "ERROR wrong_handshake\n");
        return;
    }
    char *text = (char *)line + offset;
    if (read_int(&text, 1, BUYERS, &number) || *text)
    {
        queue_text(peer, "ERROR wrong_handshake\n");
        return;
    }
    enum PeerRole wanted;
    if (!strcmp(role, "buyer") && number >= 1 && number <= server->shop.count)
    {
        wanted = BUYER;
    }
    else if (!strcmp(role, "seller") && number >= 1 && number <= DEPTS)
    {
        wanted = SELLER;
    }
    else
    {
        queue_text(peer, "ERROR wrong_role\n");
        return;
    }
    int actor = wanted == BUYER ? DEPTS + number - 1 : number - 1;
    if (find_actor(server, actor))
    {
        queue_text(peer, "ERROR role_busy\n");
        return;
    }
    peer->role = wanted;
    peer->number = number - 1;
    char welcome[128];
    if (wanted == SELLER)
    {
        snprintf(welcome, sizeof(welcome), "WELCOME SELLER %d\n", number);
    }
    else
    {
        struct Buyer *buyer = &server->shop.buyers[number - 1];
        snprintf(welcome, sizeof(welcome), "WELCOME BUYER %d %d %d %d %d %d\n",
            number, buyer->length, buyer->route[0] + 1,
            buyer->route[1] + 1, buyer->route[2] + 1, buyer->position);
    }
    queue_text(peer, welcome);
    if (server->task.actor == actor && !server->task.done)
    {
        queue_text(peer, server->task.text);
    }
    event(server, "Подключение: %s=%d", wanted == BUYER ? "покупатель" : "продавец", number);
}

static void process_line(struct Server *server, struct Peer *peer, const char *line)
{
    if (peer->role == UNKNOWN)
    {
        identify(server, peer, line);
        return;
    }
    if (peer->role == WATCHER)
    {
        return;
    }
    int id, result;
    char *text = (char *)line + 7;
    if (strncmp(line, "RESULT ", 7) || read_int(&text, 1, INT_MAX, &id) ||
        read_int(&text, 0, 10000, &result) || *text)
    {
        return;
    }
    struct Task *task = &server->task;
    if (!task->done && task->actor == actor_id(peer) && task->id == (unsigned int)id)
    {
        task->result = result;
        task->done = 1;
    }
}

static void read_peer(struct Server *server, struct Peer *peer)
{
    char buffer[512];
    ssize_t count = recv(peer->fd, buffer, sizeof(buffer), 0);
    if (count < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK))
    {
        return;
    }
    if (count <= 0)
    {
        drop_peer(peer);
        return;
    }
    for (ssize_t i = 0; i < count; i++)
    {
        char ch = buffer[i];
        if (ch == '\n')
        {
            peer->input[peer->input_len] = '\0';
            if (peer->input_len && peer->input[peer->input_len - 1] == '\r')
            {
                peer->input[peer->input_len - 1] = '\0';
            }
            process_line(server, peer, peer->input);
            peer->input_len = 0;
            if (peer->fd < 0)
            {
                return;
            }
        }
        else if (ch != '\0' && peer->input_len < sizeof(peer->input) - 1)
        {
            peer->input[peer->input_len++] = ch;
        }
        else
        {
            drop_peer(peer);
            return;
        }
    }
}

// Одно место в массиве соответствует одному подключению
static void accept_clients(struct Server *server)
{
    for (int i = 0; i < PEERS; i++)
    {
        struct Peer *peer = &server->peers[i];
        if (peer->fd >= 0)
        {
            continue;
        }
        int fd = accept(server->listener, NULL, NULL);
        if (fd < 0)
        {
            return;
        }
        if (nonblocking(fd))
        {
            close(fd);
            continue;
        }
        memset(peer, 0, sizeof(*peer));
        peer->fd = fd;
    }
    // Не оставляем лишние подключения в очереди listen
    int fd = accept(server->listener, NULL, NULL);
    if (fd >= 0)
    {
        close(fd);
    }
}

// Обслуживаем сокеты, даже если модель ждёт ответ участника
static void pump(struct Server *server, int timeout)
{
    struct pollfd fds[PEERS + 1];
    for (int i = 0; i < PEERS; i++)
    {
        struct Peer *peer = &server->peers[i];
        if (peer->fd >= 0)
        {
            prepare_replay(server, peer);
        }
        fds[i] = (struct pollfd){ .fd = peer->fd, .events = POLLIN };
        if (peer->output_len > 0)
        {
            fds[i].events |= POLLOUT;
        }
    }
    fds[PEERS] = (struct pollfd){ .fd = server->listener, .events = POLLIN };
    int ready = poll(fds, PEERS + 1, timeout);
    if (ready <= 0)
    {
        if (ready < 0 && errno != EINTR)
        {
            server->shop.error = 1;
        }
        return;
    }
    for (int i = 0; i < PEERS; i++)
    {
        struct Peer *peer = &server->peers[i];
        short events = fds[i].revents;
        if (events & POLLIN)
        {
            read_peer(server, peer);
        }
        if (peer->fd < 0)
        {
            continue;
        }
        if (events & (POLLERR | POLLHUP | POLLNVAL))
        {
            drop_peer(peer);
        }
        else if ((events & POLLOUT) && peer->output_len > 0)
        {
            ssize_t sent = send(peer->fd, peer->output,
                peer->output_len, MSG_NOSIGNAL);
            if (sent > 0)
            {
                peer->output_len -= (size_t)sent;
                memmove(peer->output, peer->output + sent, peer->output_len);
            }
            else if (sent == 0 || (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK))
            {
                drop_peer(peer);
            }
        }
    }
    if (fds[PEERS].revents & POLLIN)
    {
        accept_clients(server);
    }
}

// Начинаем моделирование только после подключения всех участников
static int wait_actors(struct Server *server)
{
    while (!interrupted && !server->shop.error)
    {
        int all = 1;
        for (int i = 0; i < DEPTS + server->shop.count; i++)
        {
            if (!find_actor(server, i))
            {
                all = 0;
                break;
            }
        }
        if (all)
        {
            return 0;
        }
        pump(server, 100);
    }
    return -1;
}

// Задание выполняет клиент, сервер проверяет и принимает результат
static int ask(struct Server *server, int actor, const char *format, ...)
{
    struct Task *task = &server->task;
    task->id = ++server->next_id;
    int prefix = snprintf(task->text, sizeof(task->text), "TASK %u ", task->id);
    va_list args;
    va_start(args, format);
    int length = vsnprintf(task->text + prefix, sizeof(task->text) - (size_t)prefix, format, args);
    va_end(args);
    if (length < 0 || (size_t)length >= sizeof(task->text) - (size_t)prefix - 1)
    {
        server->shop.error = 1;
        return -1;
    }
    task->text[prefix + length] = '\n';
    task->text[prefix + length + 1] = '\0';
    task->actor = actor;
    task->done = 0;
    notify_actor(server, actor, task->text);
    while (!interrupted && !server->shop.error && !task->done)
    {
        pump(server, 100);
    }
    task->actor = -1;
    return task->done ? task->result : -1;
}

static void enqueue(struct Server *server, int who, int department)
{
    struct Shop *s = &server->shop;
    struct Dept *dep = &s->depts[department];
    event(server, "Переход: покупатель=%d отдел=%d", who + 1, department + 1);
    if (dep->buyer >= 0 || dep->head < dep->tail)
    {
        event(server, "Очередь: покупатель=%d отдел=%d позиция=%d",
            who + 1, department + 1, dep->tail - dep->head + 1);
    }
    dep->queue[dep->tail++] = who;
    s->buyers[who].state = WAITING;
}

static void close_entrance(struct Server *server)
{
    struct Shop *s = &server->shop;
    s->closed = 1;
    event(server, "Вход закрыт");
    for (int i = 0; i < s->count; i++)
    {
        if (s->buyers[i].state == FUTURE)
        {
            s->buyers[i].state = REJECTED;
            event(server, "Отказ: покупатель=%d причина=вход_закрыт плановое_прибытие=%" PRId64,
                i + 1, (int64_t)i * s->interval);
            notify_actor(server, DEPTS + i, "INFO Вход закрыт, покупатель не допущен\n");
        }
    }
}

static int finish_sales(struct Server *server)
{
    struct Shop *s = &server->shop;
    for (int d = 0; d < DEPTS; d++)
    {
        struct Dept *dep = &s->depts[d];
        if (dep->buyer < 0 || dep->finish != s->now)
        {
            continue;
        }
        int who = dep->buyer;
        event(server, "Конец: покупатель=%d отдел=%d", who + 1, d + 1);
        notify_actor(server, d, "INFO Обслуживание завершено\n");
        notify_actor(server, DEPTS + who, "INFO Покупка завершена\n");
        dep->buyer = -1;
        dep->purchases++;
        struct Buyer *buyer = &s->buyers[who];
        buyer->position++;
        if (buyer->position == buyer->length)
        {
            buyer->state = LEFT;
            event(server, "Выход: покупатель=%d план=выполнен", who + 1);
            notify_actor(server, DEPTS + who, "INFO Маршрут выполнен\n");
        }
        else
        {
            buyer->state = READY;
            int next = ask(server, DEPTS + who, "BUY %d", buyer->position);
            if (next != buyer->route[buyer->position] + 1)
            {
                return -1;
            }
            enqueue(server, who, next - 1);
        }
    }
    return 0;
}

static int arrivals(struct Server *server)
{
    struct Shop *s = &server->shop;
    for (int i = 0; i < s->count; i++)
    {
        struct Buyer *buyer = &s->buyers[i];
        if (buyer->state != FUTURE || (int64_t)i * s->interval != s->now)
        {
            continue;
        }
        int department = ask(server, DEPTS + i, "BUY 0");
        if (department != buyer->route[0] + 1)
        {
            return -1;
        }
        char plan[16] = "";
        size_t used = 0;
        for (int j = 0; j < buyer->length; j++)
        {
            int n = snprintf(plan + used, sizeof(plan) - used, "%s%d",
                j ? "->" : "", buyer->route[j] + 1);
            if (n < 0 || (size_t)n >= sizeof(plan) - used)
            {
                return -1;
            }
            used += (size_t)n;
        }
        event(server, "Вход: покупатель=%d", i + 1);
        event(server, "План: покупатель=%d отделы=%s", i + 1, plan);
        enqueue(server, i, department - 1);
    }
    return 0;
}

static int start_sales(struct Server *server)
{
    struct Shop *s = &server->shop;
    for (int d = 0; d < DEPTS; d++)
    {
        struct Dept *dep = &s->depts[d];
        if (dep->buyer >= 0 || dep->head == dep->tail)
        {
            continue;
        }
        int duration = ask(server, d, "SERVE %d %d %u %d",
            dep->min, dep->max, s->seed, dep->purchases);
        if (duration < dep->min || duration > dep->max)
        {
            return -1;
        }
        int who = dep->queue[dep->head++];
        dep->buyer = who;
        dep->finish = s->now + duration;
        s->buyers[who].state = SERVING;
        event(server, "Начало: покупатель=%d отдел=%d окончание=%" PRId64,
            who + 1, d + 1, dep->finish);
        notify_actor(server, DEPTS + who, "INFO Началось обслуживание\n");
    }
    return 0;
}

static int64_t milliseconds(void)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (int64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000;
}

// Реальная задержка не останавливает приём новых наблюдателей
static void pause_ticks(struct Server *server, int64_t ticks)
{
    int64_t deadline = milliseconds() + ticks * server->shop.delay;
    while (!interrupted && !server->shop.error)
    {
        int64_t remaining = deadline - milliseconds();
        if (remaining <= 0)
        {
            break;
        }
        pump(server, remaining > 100 ? 100 : (int)remaining);
    }
}

// Модельное время всегда меняет только сервер
static int simulate(struct Server *server)
{
    struct Shop *s = &server->shop;
    event(server, "Запуск: отделов=3 продавцов_на_отдел=1 покупателей=%d интервал=%d закрытие=%d задержка_мс=%d seed=%u",
        s->count, s->interval, s->closing, s->delay, s->seed);
    for (int d = 0; d < DEPTS; d++)
    {
        event(server, "Параметры: отдел=%d минимум=%d максимум=%d",
            d + 1, s->depts[d].min, s->depts[d].max);
    }
    while (!interrupted && !s->error)
    {
        int64_t next = s->closed ? NEVER : s->closing;
        if (!s->closed)
        {
            for (int i = 0; i < s->count; i++)
            {
                int64_t arrival = (int64_t)i * s->interval;
                if (s->buyers[i].state == FUTURE && arrival < next)
                {
                    next = arrival;
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
            break;
        }
        pause_ticks(server, next - s->now);
        if (interrupted || s->error)
        {
            break;
        }
        s->now = next;
        if (!s->closed && next == s->closing)
        {
            close_entrance(server);
        }
        if (finish_sales(server) || (!s->closed && arrivals(server)) || start_sales(server))
        {
            s->error = 1;
            break;
        }
    }
    return s->error ? -1 : 0;
}

static void summary(struct Server *server, const char *reason)
{
    struct Shop *s = &server->shop;
    int entered = 0, left = 0, rejected = 0, future = 0;
    for (int i = 0; i < s->count; i++)
    {
        enum BuyerState state = s->buyers[i].state;
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
    event(server, "Итог: причина=%s", reason);
    event(server, "Покупатели: всего=%d вошли=%d вышли=%d внутри=%d не_допущены=%d ещё_не_пришли=%d",
        s->count, entered, left, entered - left, rejected, future);
    for (int d = 0; d < DEPTS; d++)
    {
        struct Dept *dep = &s->depts[d];
        event(server, "Отдел: номер=%d покупок=%d очередь=%d обслуживается=%d",
            d + 1, dep->purchases, dep->tail - dep->head, dep->buyer + 1);
    }
    for (int i = 0; i < s->count; i++)
    {
        struct Buyer *buyer = &s->buyers[i];
        if (buyer->state == WAITING || buyer->state == SERVING || buyer->state == READY)
        {
            event(server, "Остался: покупатель=%d состояние=%s отдел=%d выполнено=%d из=%d",
                i + 1, buyer->state == WAITING ? "очередь" :
                buyer->state == SERVING ? "обслуживание" : "переход",
                buyer->route[buyer->position] + 1, buyer->position, buyer->length);
        }
    }
}

static int listener(const char *ip, int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        return -1;
    }
    int yes = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    struct sockaddr_in address = { .sin_family = AF_INET, .sin_port = htons((uint16_t)port) };
    if (inet_pton(AF_INET, ip, &address.sin_addr) != 1 ||
        bind(fd, (struct sockaddr *)&address, sizeof(address)) != 0 ||
        listen(fd, 128) != 0 || nonblocking(fd) != 0)
    {
        close(fd);
        return -1;
    }
    return fd;
}

// Перед закрытием сервер успевает отправить STOP и последние события
static void stop_clients(struct Server *server)
{
    close(server->listener);
    server->listener = -1;
    for (int i = 0; i < PEERS; i++)
    {
        struct Peer *peer = &server->peers[i];
        if (peer->fd >= 0 && peer->role != WATCHER)
        {
            queue_text(peer, "STOP\n");
        }
    }
    int64_t deadline = milliseconds() + 2000;
    while (1)
    {
        int pending = 0;
        for (int i = 0; i < PEERS; i++)
        {
            struct Peer *peer = &server->peers[i];
            if (peer->fd < 0)
            {
                continue;
            }
            prepare_replay(server, peer);
            if (peer->role == WATCHER && peer->replay_pos == server->history_count)
            {
                // STOP идёт после последних записей истории
                if (peer->replay_pos >= 0)
                {
                    queue_text(peer, "STOP\n");
                    peer->replay_pos = -1;
                }
            }
            if (peer->output_len > 0)
            {
                pending++;
            }
        }
        if (!pending)
        {
            break;
        }
        if (milliseconds() >= deadline)
        {
            break;
        }
        pump(server, 10);
    }
    for (int i = 0; i < PEERS; i++)
    {
        drop_peer(&server->peers[i]);
    }
}

int main(int argc, char **argv)
{
    if (argc == 2 && !strcmp(argv[1], "--help"))
    {
        dprintf(STDOUT_FILENO,
            "Запуск: %s BIND_IP PORT INPUT_FILE LOG_FILE DELAY_MS SEED [--trace]\n"
            "После запуска подключите 3 продавцов и N покупателей через ./client.\n", argv[0]);
        return 0;
    }
    long port, delay, seed;
    int trace = argc == 8 && !strcmp(argv[7], "--trace");
    if ((argc != 7 && !trace) || argument(argv[2], 1, 65535, &port) ||
        argument(argv[5], 0, 1000, &delay) || argument(argv[6], 0, INT_MAX, &seed))
    {
        dprintf(STDERR_FILENO, "Запуск: ./server BIND_IP PORT INPUT LOG DELAY_MS SEED [--trace]\n");
        return 1;
    }
    struct Server *server = calloc(1, sizeof(*server));
    if (!server)
    {
        return 1;
    }
    for (int i = 0; i < PEERS; i++)
    {
        server->peers[i].fd = -1;
    }
    server->listener = -1;
    server->task.actor = -1;
    server->shop.trace = trace;
    server->shop.delay = (int)delay;
    server->shop.seed = (unsigned int)seed;
    struct stat source;
    if (load_shop(argv[3], &server->shop, &source))
    {
        free(server);
        return 1;
    }
    server->shop.log_fd = open_log(argv[4], &source);
    if (server->shop.log_fd < 0)
    {
        free(server);
        return 1;
    }
    struct sigaction sa = { .sa_handler = on_signal };
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, NULL);
    struct stat out;
    if (trace && !fstat(STDOUT_FILENO, &out) && S_ISFIFO(out.st_mode))
    {
        nonblocking(STDOUT_FILENO);
    }
    server->listener = listener(argv[1], (int)port);
    if (server->listener < 0)
    {
        perror("Запуск TCP-сервера");
        close(server->shop.log_fd);
        free(server);
        return 1;
    }
    dprintf(STDERR_FILENO, "Сервер слушает %s:%ld, покупателей: %d\n",
        argv[1], port, server->shop.count);
    int error = 0;
    if (wait_actors(server) != 0 && !interrupted)
    {
        error = 1;
    }
    if (!error && !interrupted && simulate(server) != 0)
    {
        error = 1;
    }
    const char *reason = interrupted == SIGINT ? "SIGINT" :
        interrupted == SIGTERM ? "SIGTERM" : error || server->shop.error ?
        "ошибка_взаимодействия" : "завершено";
    summary(server, reason);
    dprintf(STDERR_FILENO, "Сервер завершает работу: %s\n", reason);
    stop_clients(server);
    if (close(server->shop.log_fd) != 0)
    {
        error = 1;
    }
    error |= server->shop.error;
    free(server->history);
    free(server);
    return interrupted ? 128 + interrupted : error ? 1 : 0;
}
