#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <semaphore.h>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <string>

#define SHM_NAME "/my_shared_mem"
#define MAX_STR_SIZE 1024

// Shared data structure in the shared memory region using POSIX semaphore.
struct SharedData
{
    sem_t sem;              // POSIX unnamed semaphore for protecting access.
    int value;              // An integer value.
    size_t str_len;         // Actual length of the stored string.
    char str[MAX_STR_SIZE]; // Buffer for string data.
};

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0] << " writer|reader" << std::endl;
        return EXIT_FAILURE;
    }

    bool isWriter = (std::string(argv[1]) == "writer");

    // Open (or create) the shared memory object.
    int shm_fd;
    if (isWriter)
    {
        shm_fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
    }
    else
    {
        shm_fd = shm_open(SHM_NAME, O_RDWR, 0666);
    }
    if (shm_fd == -1)
    {
        perror("shm_open");
        return EXIT_FAILURE;
    }

    // For the writer, set the shared memory size.
    if (isWriter)
    {
        if (ftruncate(shm_fd, sizeof(SharedData)) == -1)
        {
            perror("ftruncate");
            return EXIT_FAILURE;
        }
    }

    // Map the shared memory region.
    void *ptr = mmap(nullptr, sizeof(SharedData),
                     PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (ptr == MAP_FAILED)
    {
        perror("mmap");
        return EXIT_FAILURE;
    }
    SharedData *shared = static_cast<SharedData *>(ptr);

    if (isWriter)
    {
        // Initialize the POSIX semaphore for process sharing with an initial value of 1.
        if (sem_init(&shared->sem, 1, 1) == -1)
        {
            perror("sem_init");
            return EXIT_FAILURE;
        }
        // Initialize other fields.
        shared->value = 0;
        shared->str_len = 0;
        memset(shared->str, 0, MAX_STR_SIZE);
    }

    if (isWriter)
    {
        // Writer: repeatedly update the shared data.
        for (int i = 0; i < 5; i++)
        {
            // Lock the semaphore.
            if (sem_wait(&shared->sem) == -1)
            {
                perror("sem_wait");
                return EXIT_FAILURE;
            }
            shared->value = i;
            std::string message = "Hello from writer, message number " + std::to_string(i);
            shared->str_len = message.size();
            if (shared->str_len > MAX_STR_SIZE - 1)
                shared->str_len = MAX_STR_SIZE - 1;
            strncpy(shared->str, message.c_str(), shared->str_len);
            shared->str[shared->str_len] = '\0';
            std::cout << "Writer wrote: value = " << shared->value
                      << ", str = \"" << shared->str << "\"" << std::endl;
            // Unlock the semaphore.
            if (sem_post(&shared->sem) == -1)
            {
                perror("sem_post");
                return EXIT_FAILURE;
            }
            sleep(1);
        }
    }
    else
    {
        // Reader: repeatedly read the shared data.
        for (int i = 0; i < 5; i++)
        {
            if (sem_wait(&shared->sem) == -1)
            {
                perror("sem_wait");
                return EXIT_FAILURE;
            }
            int val = shared->value;
            size_t len = shared->str_len;
            std::string message(shared->str, len);
            std::cout << "Reader read: value = " << val
                      << ", str = \"" << message << "\"" << std::endl;
            if (sem_post(&shared->sem) == -1)
            {
                perror("sem_post");
                return EXIT_FAILURE;
            }
            sleep(1);
        }
    }

    // Clean up: unmap and close the shared memory.
    munmap(ptr, sizeof(SharedData));
    close(shm_fd);
    if (isWriter)
    {
        // Optionally remove the shared memory object.
        shm_unlink(SHM_NAME);
    }
    return EXIT_SUCCESS;
}
