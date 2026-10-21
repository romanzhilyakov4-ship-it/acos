#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define DEPARTMENTS 3
#define MAX_BUYERS 100
#define MAX_TIME 100000
#define MAX_SERVICE 10000
#define INPUT_SIZE 65536

// Состояние покупателя в последовательной модели
enum BuyerState
{
    NOT_ARRIVED,
    WAITING,
    SERVING,
    LEFT,
    REJECTED
};

// План покупок и текущее положение покупателя
struct Buyer
{
    int route[DEPARTMENTS];
    int length;
    int position;
    enum BuyerState state;
};

// Продавец и очередь одного отдела
struct Department
{
    int minimum;
    int maximum;
    int buyer;
    int finish_time;
    int queue[MAX_BUYERS];
    int head;
    int tail;
    int purchases;
};

// Все данные одного запуска модели
struct Shop
{
    int count;
    int interval;
    int closing_time;
    struct Buyer buyers[MAX_BUYERS];
    struct Department departments[DEPARTMENTS];
};

static volatile sig_atomic_t stop_signal;
static int log_fd = -1;
static int output_error;

// Дописывает строку, отличая сигнал от ошибки и проверяя остановку при ожидании
static int write_text(int fd, const char *text, size_t length, int interruptible)
{
    size_t used = 0;
    while (used < length)
    {
        if (interruptible && stop_signal)
        {
            return 0;
        }
        ssize_t written = write(fd, text + used, length - used);
        if (written > 0)
        {
            used += (size_t)written;
        }
        else if (written < 0 && errno == EINTR)
        {
            continue;
        }
        else if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            struct pollfd output;
            output.fd = fd;
            output.events = POLLOUT;
            output.revents = 0;
            if (poll(&output, 1, 100) < 0 && errno != EINTR)
            {
                return -1;
            }
        }
        else
        {
            if (written == 0)
            {
                errno = EIO;
            }
            return -1;
        }
    }
    return 0;
}

// Обработчик только сообщает основному циклу о прерывании
static void handle_signal(int number)
{
    stop_signal = number;
}

// Записывает одно событие одновременно в терминал и журнал
static void event(int time, const char *format, ...)
{
    char message[1024];
    va_list arguments;
    va_start(arguments, format);
    int length = vsnprintf(message, sizeof(message), format, arguments);
    va_end(arguments);

    if (length < 0 || (size_t)length >= sizeof(message))
    {
        output_error = 1;
        return;
    }

    char line[1056];
    int line_length = snprintf(line, sizeof(line), "[t=%d] %s\n", time, message);
    if (line_length < 0 || (size_t)line_length >= sizeof(line))
    {
        output_error = 1;
        return;
    }
    if (write_text(STDOUT_FILENO, line, (size_t)line_length, 1) < 0)
    {
        output_error = 1;
    }
    if (write_text(log_fd, line, (size_t)line_length, 0) < 0)
    {
        output_error = 1;
    }
}

// Читает очередное целое число без принятия повреждённых токенов
static int next_number(char **text, long minimum, long maximum, long *value)
{
    char *start = *text;
    while (*start == ' ' || *start == '\t' || *start == '\r' || *start == '\n')
    {
        start++;
    }
    errno = 0;
    char *end;
    long number = strtol(start, &end, 10);
    if (end == start || errno == ERANGE || number < minimum || number > maximum)
    {
        return -1;
    }
    if (*end != '\0' && *end != ' ' && *end != '\t' && *end != '\r' && *end != '\n')
    {
        return -1;
    }
    *text = end;
    *value = number;
    return 0;
}

// Проверяет отдельный числовой аргумент командной строки
static int argument_number(char *text, long maximum, long *value)
{
    if (next_number(&text, 0, maximum, value) < 0)
    {
        return -1;
    }
    return *text == '\0' ? 0 : -1;
}

// Загружает обычный файл через дескриптор и проверяет все параметры
static int load_shop(const char *path, struct Shop *shop, struct stat *input_info)
{
    char buffer[INPUT_SIZE];
    size_t used = 0;
    int fd = open(path, O_RDONLY | O_NONBLOCK);
    if (fd < 0)
    {
        perror("Не удалось открыть входной файл");
        return -1;
    }
    if (fstat(fd, input_info) < 0 || !S_ISREG(input_info->st_mode))
    {
        dprintf(STDERR_FILENO, "Вход должен быть обычным файлом\n");
        close(fd);
        return -1;
    }
    while (!stop_signal)
    {
        ssize_t length = read(fd, buffer + used, sizeof(buffer) - used - 1);
        if (length < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            perror("Ошибка чтения входного файла");
            close(fd);
            return -1;
        }
        if (length == 0)
        {
            break;
        }
        used += (size_t)length;
        if (used == sizeof(buffer) - 1)
        {
            dprintf(STDERR_FILENO, "Входной файл слишком большой\n");
            close(fd);
            return -1;
        }
    }
    if (close(fd) < 0)
    {
        perror("Ошибка закрытия входного файла");
        return -1;
    }
    if (stop_signal)
    {
        return -1;
    }
    if (memchr(buffer, '\0', used) != NULL)
    {
        dprintf(STDERR_FILENO, "Входной файл содержит нулевой байт\n");
        return -1;
    }
    buffer[used] = '\0';
    char *text = buffer;
    long number;
    const char *field = "количество покупателей N";
    int department_number = 0;
    int buyer_number = 0;
    int route_step = 0;

    if (next_number(&text, 0, MAX_BUYERS, &number) < 0)
    {
        goto invalid;
    }
    shop->count = (int)number;
    field = "интервал появления INTERVAL";
    if (next_number(&text, 0, MAX_TIME, &number) < 0)
    {
        goto invalid;
    }
    shop->interval = (int)number;
    field = "время закрытия CLOSE";
    if (next_number(&text, 0, MAX_TIME, &number) < 0)
    {
        goto invalid;
    }
    shop->closing_time = (int)number;

    for (int d = 0; d < DEPARTMENTS; d++)
    {
        department_number = d + 1;
        struct Department *department = &shop->departments[d];
        field = "минимальное время обслуживания MIN";
        if (next_number(&text, 1, MAX_SERVICE, &number) < 0)
        {
            goto invalid;
        }
        department->minimum = (int)number;
        field = "максимальное время обслуживания MAX (должно быть не меньше MIN)";
        if (next_number(&text, department->minimum, MAX_SERVICE, &number) < 0)
        {
            goto invalid;
        }
        department->maximum = (int)number;
        department->buyer = -1;
    }

    for (int i = 0; i < shop->count; i++)
    {
        department_number = 0;
        struct Buyer *buyer = &shop->buyers[i];
        buyer_number = i + 1;
        route_step = 0;
        field = "длина маршрута K";
        if (next_number(&text, 1, DEPARTMENTS, &number) < 0)
        {
            goto invalid;
        }
        buyer->length = (int)number;
        for (int j = 0; j < buyer->length; j++)
        {
            route_step = j + 1;
            field = "номер отдела (допустимы 1..3)";
            if (next_number(&text, 1, DEPARTMENTS, &number) < 0)
            {
                goto invalid;
            }
            buyer->route[j] = (int)number - 1;
            for (int k = 0; k < j; k++)
            {
                if (buyer->route[k] == buyer->route[j])
                {
                    field = "повтор отдела в маршруте";
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
        field = "лишние данные после последнего маршрута";
        buyer_number = 0;
        route_step = 0;
        goto invalid;
    }
    return 0;

invalid:
    dprintf(STDERR_FILENO, "Некорректные данные: %s\n", field);
    if (buyer_number > 0)
    {
        dprintf(STDERR_FILENO, "Покупатель: %d\n", buyer_number);
        if (route_step > 0)
        {
            dprintf(STDERR_FILENO, "Пункт маршрута: %d\n", route_step);
        }
    }
    if (department_number > 0)
    {
        dprintf(STDERR_FILENO, "Отдел: %d\n", department_number);
    }
    return -1;
}

// Защищает исходные данные от перезаписи, включая ссылки на тот же файл
static int open_log(const char *path, const struct stat *input_info)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_NONBLOCK, 0644);
    if (fd < 0)
    {
        perror("Не удалось открыть журнал");
        return -1;
    }
    struct stat info;
    if (fstat(fd, &info) < 0 || !S_ISREG(info.st_mode))
    {
        dprintf(STDERR_FILENO, "Журнал должен быть обычным файлом\n");
        close(fd);
        return -1;
    }
    if (info.st_dev == input_info->st_dev && info.st_ino == input_info->st_ino)
    {
        dprintf(STDERR_FILENO, "Журнал не должен совпадать с входным файлом\n");
        close(fd);
        return -1;
    }
    if (ftruncate(fd, 0) < 0)
    {
        perror("Не удалось очистить журнал");
        close(fd);
        return -1;
    }
    return fd;
}

// Каждый покупатель попадает в очередь одного отдела не более одного раза
static void move_to_department(struct Shop *shop, int number, int time)
{
    struct Buyer *buyer = &shop->buyers[number];
    int d = buyer->route[buyer->position];
    struct Department *department = &shop->departments[d];
    event(time, "Переход: покупатель=%d отдел=%d", number + 1, d + 1);
    if (department->buyer != -1 || department->head < department->tail)
    {
        event(time, "Очередь: покупатель=%d отдел=%d позиция=%d", number + 1,
              d + 1, department->tail - department->head + 1);
    }
    department->queue[department->tail++] = number;
    buyer->state = WAITING;
}

// Завершает все обслуживания текущего такта до новых назначений продавцов
static void finish_services(struct Shop *shop, int time)
{
    for (int d = 0; d < DEPARTMENTS; d++)
    {
        struct Department *department = &shop->departments[d];
        int number = department->buyer;
        if (number == -1 || department->finish_time != time)
        {
            continue;
        }
        struct Buyer *buyer = &shop->buyers[number];
        event(time, "Конец: покупатель=%d отдел=%d", number + 1, d + 1);
        department->buyer = -1;
        department->purchases++;
        buyer->position++;
        if (buyer->position == buyer->length)
        {
            buyer->state = LEFT;
            event(time, "Выход: покупатель=%d план=выполнен", number + 1);
        }
        else
        {
            move_to_department(shop, number, time);
        }
    }
}

// Начинает обслуживание первых покупателей очередей свободных отделов
static void start_services(struct Shop *shop, int time)
{
    for (int d = 0; d < DEPARTMENTS; d++)
    {
        struct Department *department = &shop->departments[d];
        if (department->buyer != -1 || department->head == department->tail)
        {
            continue;
        }
        int number = department->queue[department->head++];
        int duration = department->minimum;
        if (department->minimum != department->maximum)
        {
            duration += rand() % (department->maximum - department->minimum + 1);
        }
        department->buyer = number;
        department->finish_time = time + duration;
        shop->buyers[number].state = SERVING;
        event(time, "Начало: покупатель=%d отдел=%d окончание=%d", number + 1,
              d + 1, department->finish_time);
    }
}

// Сохраняет статистику и состояние незавершённых покупок
static void summary(const struct Shop *shop, int time, const char *reason)
{
    int entered = 0;
    int left = 0;
    int rejected = 0;
    int future = 0;
    for (int i = 0; i < shop->count; i++)
    {
        enum BuyerState state = shop->buyers[i].state;
        if (state == NOT_ARRIVED)
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
    event(time, "Итог: причина=%s", reason);
    event(time, "Покупатели: всего=%d вошли=%d вышли=%d внутри=%d не_допущены=%d ещё_не_пришли=%d",
          shop->count, entered, left, entered - left, rejected, future);
    for (int d = 0; d < DEPARTMENTS; d++)
    {
        const struct Department *department = &shop->departments[d];
        event(time, "Отдел: номер=%d покупок=%d очередь=%d обслуживается=%d", d + 1,
              department->purchases, department->tail - department->head,
              department->buyer + 1);
    }
    for (int i = 0; i < shop->count; i++)
    {
        const struct Buyer *buyer = &shop->buyers[i];
        if (buyer->state == WAITING || buyer->state == SERVING)
        {
            event(time, "Остался: покупатель=%d состояние=%s отдел=%d выполнено=%d из=%d",
                  i + 1, buyer->state == WAITING ? "очередь" : "обслуживание",
                  buyer->route[buyer->position] + 1, buyer->position, buyer->length);
        }
    }
}

// Моделирует целые такты, продолжая покупки после закрытия входа
static int simulate(struct Shop *shop, int delay, unsigned int seed)
{
    int time = 0;
    int inside = 0;
    int closed = 0;
    srand(seed);
    event(0, "Запуск: отделов=3 продавцов_на_отдел=1 покупателей=%d интервал=%d закрытие=%d задержка_мс=%d seed=%u",
          shop->count, shop->interval, shop->closing_time, delay, seed);
    for (int d = 0; d < DEPARTMENTS; d++)
    {
        event(0, "Параметры: отдел=%d минимум=%d максимум=%d", d + 1,
              shop->departments[d].minimum, shop->departments[d].maximum);
    }

    while (!stop_signal && !output_error)
    {
        if (!closed && time == shop->closing_time)
        {
            closed = 1;
            event(time, "Вход закрыт");
            for (int i = 0; i < shop->count; i++)
            {
                if (shop->buyers[i].state == NOT_ARRIVED)
                {
                    shop->buyers[i].state = REJECTED;
                    event(time, "Отказ: покупатель=%d причина=вход_закрыт плановое_прибытие=%d",
                          i + 1, i * shop->interval);
                }
            }
        }

        finish_services(shop, time);
        for (int i = 0; i < shop->count; i++)
        {
            struct Buyer *buyer = &shop->buyers[i];
            if (!closed && buyer->state == NOT_ARRIVED && i * shop->interval == time)
            {
                event(time, "Вход: покупатель=%d", i + 1);
                char plan[16];
                int used = 0;
                for (int j = 0; j < buyer->length; j++)
                {
                    used += snprintf(plan + used, sizeof(plan) - (size_t)used,
                                     "%s%d", j == 0 ? "" : "->", buyer->route[j] + 1);
                }
                event(time, "План: покупатель=%d отделы=%s", i + 1, plan);
                move_to_department(shop, i, time);
            }
        }
        start_services(shop, time);

        inside = 0;
        for (int i = 0; i < shop->count; i++)
        {
            if (shop->buyers[i].state == WAITING || shop->buyers[i].state == SERVING)
            {
                inside++;
            }
        }
        if (closed && inside == 0)
        {
            break;
        }
        if (delay > 0 && !stop_signal && !output_error)
        {
            struct timespec remaining;
            remaining.tv_sec = delay / 1000;
            remaining.tv_nsec = (delay % 1000) * 1000000L;
            while (nanosleep(&remaining, &remaining) < 0)
            {
                if (errno != EINTR)
                {
                    output_error = 1;
                    break;
                }
                if (stop_signal)
                {
                    break;
                }
            }
        }
        if (!stop_signal && !output_error)
        {
            time++;
        }
    }

    const char *reason = "завершено";
    if (output_error)
    {
        reason = "ошибка_ввода_вывода";
    }
    else if (stop_signal == SIGINT)
    {
        reason = "SIGINT";
    }
    else if (stop_signal == SIGTERM)
    {
        reason = "SIGTERM";
    }
    summary(shop, time, reason);
    if (output_error)
    {
        dprintf(STDERR_FILENO, "Ошибка вывода или сохранения журнала\n");
        return 1;
    }
    return stop_signal ? 128 + stop_signal : 0;
}

// Проверяет интерфейс запуска и освобождает ресурсы после моделирования
int main(int argc, char **argv)
{
    if (argc == 2 && strcmp(argv[1], "--help") == 0)
    {
        if (dprintf(STDOUT_FILENO,
                "Запуск: %s INPUT_FILE LOG_FILE DELAY_MS SEED\n"
                "Файл: N INTERVAL CLOSE, затем 3 пары MIN MAX, затем N маршрутов K D1 ... DK\n"
                "N: 0..100; INTERVAL и CLOSE: 0..100000; MIN и MAX: 1..10000\n"
                "K: 1..3; отделы: 1..3 без повторений; задержка: 0..1000 мс\n"
                "Время прибытия покупателя i: (i - 1) * INTERVAL; при t >= CLOSE вход закрыт\n",
                argv[0]) < 0)
        {
            perror("Не удалось вывести справку");
            return 1;
        }
        return 0;
    }
    if (argc != 5)
    {
        dprintf(STDERR_FILENO, "Запуск: %s INPUT_FILE LOG_FILE DELAY_MS SEED\n", argv[0]);
        return 1;
    }
    long delay;
    long seed;
    if (argument_number(argv[3], 1000, &delay) < 0 ||
        argument_number(argv[4], INT_MAX, &seed) < 0)
    {
        dprintf(STDERR_FILENO, "Задержка: 0..1000 мс; seed: 0..INT_MAX\n");
        return 1;
    }

    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_handler = handle_signal;
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGINT, &action, NULL) < 0 || sigaction(SIGTERM, &action, NULL) < 0)
    {
        perror("Не удалось установить обработчики сигналов");
        return 1;
    }
    action.sa_handler = SIG_IGN;
    if (sigaction(SIGPIPE, &action, NULL) < 0)
    {
        perror("Не удалось настроить SIGPIPE");
        return 1;
    }

    struct Shop shop;
    memset(&shop, 0, sizeof(shop));
    struct stat input_info;
    if (load_shop(argv[1], &shop, &input_info) < 0)
    {
        return stop_signal ? 128 + stop_signal : 1;
    }
    log_fd = open_log(argv[2], &input_info);
    if (log_fd < 0)
    {
        return stop_signal ? 128 + stop_signal : 1;
    }
    int stdout_flags = -1;
    struct stat stdout_info;
    if (fstat(STDOUT_FILENO, &stdout_info) < 0)
    {
        perror("Не удалось проверить стандартный вывод");
        close(log_fd);
        return 1;
    }
    // Полный канал не должен мешать замечать сигнал остановки
    if (S_ISFIFO(stdout_info.st_mode))
    {
        stdout_flags = fcntl(STDOUT_FILENO, F_GETFL);
        if (stdout_flags < 0 || fcntl(STDOUT_FILENO, F_SETFL, stdout_flags | O_NONBLOCK) < 0)
        {
            perror("Не удалось настроить стандартный вывод");
            close(log_fd);
            return 1;
        }
    }
    int result = simulate(&shop, (int)delay, (unsigned int)seed);
    if (stdout_flags >= 0 && fcntl(STDOUT_FILENO, F_SETFL, stdout_flags) < 0)
    {
        perror("Не удалось восстановить стандартный вывод");
        result = 1;
    }
    if (close(log_fd) < 0)
    {
        perror("Ошибка закрытия журнала");
        result = 1;
    }
    return result;
}
