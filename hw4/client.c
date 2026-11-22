#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <errno.h>
#include <limits.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

static volatile sig_atomic_t interrupted;

static void on_signal(int number)
{
    interrupted = number;
}

static int number(const char *text, long min, long max, long *value)
{
    errno = 0;
    char *end;
    long n = strtol(text, &end, 10);
    if (end == text || *end || errno || n < min || n > max)
    {
        return -1;
    }
    *value = n;
    return 0;
}

// Отдельный клиент подключается к серверу по указанному IP и порту
static int connect_server(const char *ip, int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        return -1;
    }
    struct sockaddr_in address =
    {
        .sin_family = AF_INET,
        .sin_port = htons((uint16_t)port)
    };
    if (inet_pton(AF_INET, ip, &address.sin_addr) != 1 ||
        connect(fd, (struct sockaddr *)&address, sizeof(address)) != 0)
    {
        close(fd);
        return -1;
    }
    return fd;
}

static int send_all(int fd, const char *line)
{
    size_t length = strlen(line), used = 0;
    while (used < length && !interrupted)
    {
        ssize_t n = send(fd, line + used, length - used, MSG_NOSIGNAL);
        if (n > 0)
        {
            used += (size_t)n;
        }
        else if (n < 0 && errno == EINTR)
        {
            continue;
        }
        else
        {
            return -1;
        }
    }
    return used == length ? 0 : -1;
}

// Расчёт не зависит от переподключений и повторной доставки задания
static int duration(unsigned int seed, unsigned int seller, unsigned int sale,
    int minimum, int maximum)
{
    uint32_t value = seed ^ (seller * UINT32_C(0x9e3779b9)) ^
        (sale * UINT32_C(0x85ebca6b));
    value ^= value >> 16;
    value *= UINT32_C(0x7feb352d);
    value ^= value >> 15;
    value *= UINT32_C(0x846ca68b);
    value ^= value >> 16;
    return minimum + (int)(value % (unsigned int)(maximum - minimum + 1));
}

// Покупатель выбирает отдел из своего маршрута, продавец назначает время
static int handle_line(int fd, const char *line, int role, int number,
    int route[3], int *known)
{
    if (!strncmp(line, "WELCOME BUYER ", 14))
    {
        int id, length, a, b, c, position;
        if (sscanf(line, "WELCOME BUYER %d %d %d %d %d %d",
            &id, &length, &a, &b, &c, &position) != 6 ||
            id != number || length < 1 || length > 3 ||
            position < 0 || position > length || a < 1 || a > 3 ||
            (length >= 2 && (b < 1 || b > 3)) ||
            (length == 3 && (c < 1 || c > 3)))
        {
            return -1;
        }
        route[0] = a;
        route[1] = b;
        route[2] = c;
        *known = length;
        printf("Покупатель %d: подключён, выполнено покупок: %d\n", number, position);
        return 0;
    }
    if (!strncmp(line, "WELCOME ", 8))
    {
        printf("Подключение установлено\n");
        return 0;
    }
    if (!strncmp(line, "EVENT ", 6) || !strncmp(line, "INFO ", 5))
    {
        printf("%s\n", strchr(line, ' ') + 1);
        return 0;
    }
    if (!strcmp(line, "STOP"))
    {
        printf("Работа завершена\n");
        return 1;
    }
    if (!strncmp(line, "ERROR ", 6))
    {
        fprintf(stderr, "%s\n", line);
        return -1;
    }
    unsigned int request;
    int step;
    int min, max;
    unsigned int seed, sale;
    char answer[80];
    int selected;
    if (role == 1 && sscanf(line, "TASK %u BUY %d", &request, &step) == 2)
    {
        if (!*known || step < 0 || step >= *known)
        {
            return -1;
        }
        printf("Покупатель %d: следующий отдел %d\n", number, route[step]);
        selected = route[step];
    }
    else if (role == 2 && sscanf(line, "TASK %u SERVE %d %d %u %u",
        &request, &min, &max, &seed, &sale) == 5)
    {
        if (min < 1 || max < min || max > 10000)
        {
            return -1;
        }
        selected = duration(seed, (unsigned int)number, sale, min, max);
        printf("Продавец %d: обслуживание займёт %d тактов\n", number, selected);
    }
    else
    {
        return -1;
    }
    snprintf(answer, sizeof(answer), "RESULT %u %d\n", request, selected);
    return send_all(fd, answer);
}

int main(int argc, char **argv)
{
    if (argc == 2 && !strcmp(argv[1], "--help"))
    {
#ifdef OBSERVER
        printf("Запуск: ./observer SERVER_IP PORT\n");
#else
        printf("Запуск: ./client SERVER_IP PORT watch | buyer NUMBER | seller NUMBER\n");
#endif
        return 0;
    }
    long port, id = 0;
    int role;
#ifdef OBSERVER
    if (argc != 3 || number(argv[2], 1, 65535, &port))
    {
        fprintf(stderr, "Запуск: ./observer SERVER_IP PORT\n");
        return 1;
    }
    role = 0;
#else
    if ((argc != 4 && argc != 5) || number(argv[2], 1, 65535, &port))
    {
        fprintf(stderr, "Запуск: ./client SERVER_IP PORT watch | buyer NUMBER | seller NUMBER\n");
        return 1;
    }
    if (argc == 4 && !strcmp(argv[3], "watch"))
    {
        role = 0;
    }
    else if (argc == 5 && !strcmp(argv[3], "buyer") && !number(argv[4], 1, 100, &id))
    {
        role = 1;
    }
    else if (argc == 5 && !strcmp(argv[3], "seller") && !number(argv[4], 1, 3, &id))
    {
        role = 2;
    }
    else
    {
        return 1;
    }
#endif
    struct in_addr address;
    if (inet_pton(AF_INET, argv[1], &address) != 1)
    {
        fprintf(stderr, "Некорректный IPv4-адрес\n");
        return 1;
    }
    struct sigaction sa = { .sa_handler = on_signal };
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, NULL);
    setvbuf(stdout, NULL, _IOLBF, 0);
    char handshake[64];
    if (role == 0)
    {
        snprintf(handshake, sizeof(handshake), "WATCH\n");
    }
    else
    {
        snprintf(handshake, sizeof(handshake), "HELLO %s %ld\n",
            role == 1 ? "buyer" : "seller", id);
    }
    int connected_once = 0;
    int route[3] = { 0 };
    int known = 0;
    while (!interrupted)
    {
        int fd = connect_server(argv[1], (int)port);
        if (fd < 0)
        {
            if (connected_once)
            {
                fprintf(stderr, "Соединение с сервером потеряно\n");
                return 1;
            }
            // Участников можно запускать до сервера
            struct timespec delay = { .tv_sec = 0, .tv_nsec = 100000000L };
            nanosleep(&delay, NULL);
            continue;
        }
        connected_once = 1;
        if (send_all(fd, handshake) != 0)
        {
            close(fd);
            continue;
        }
        char line[512], buffer[512];
        size_t used = 0;
        int result = 0;
        // TCP не сохраняет границы сообщений, поэтому собираем строки сами
        while (!interrupted && !result)
        {
            struct pollfd p = { .fd = fd, .events = POLLIN };
            int rc = poll(&p, 1, 200);
            if (rc < 0 && errno != EINTR)
            {
                break;
            }
            if (rc <= 0)
            {
                continue;
            }
            ssize_t n = recv(fd, buffer, sizeof(buffer), 0);
            if (n < 0 && errno == EINTR)
            {
                continue;
            }
            if (n <= 0)
            {
                break;
            }
            for (ssize_t i = 0; i < n && !result; i++)
            {
                if (buffer[i] == '\n')
                {
                    if (used && line[used - 1] == '\r')
                    {
                        used--;
                    }
                    line[used] = '\0';
                    result = handle_line(fd, line, role, (int)id, route, &known);
                    used = 0;
                }
                else if (buffer[i] != '\0' && used < sizeof(line) - 1)
                {
                    line[used++] = buffer[i];
                }
                else
                {
                    result = -1;
                }
            }
        }
        close(fd);
        if (result != 0)
        {
            return result > 0 ? 0 : 1;
        }
        // После потери соединения можно снова подключиться к работающему серверу
        struct timespec delay = { .tv_sec = 0, .tv_nsec = 100000000L };
        nanosleep(&delay, NULL);
    }
    return interrupted ? 128 + interrupted : 0;
}
