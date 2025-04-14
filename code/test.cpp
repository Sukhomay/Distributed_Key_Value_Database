#include <fcntl.h>      // For open(), O_ flags, etc.
#include <unistd.h>     // For lseek(), write(), read(), ftruncate(), close()
#include <sys/types.h>  // For types like ssize_t
#include <sys/stat.h>   // For file modes (S_IRUSR, S_IWUSR, etc.)
#include <string>       // For std::string
#include <iostream>     // For std::cerr, std::cout

/**
 * Writes the provided string as the first line of the file referred by the file descriptor.
 *
 * @param fd   An open file descriptor.
 * @param line The string to write as the first line. A newline will be appended
 *             if the string does not end with one.
 * @return     true on success, false on failure.
 */
bool write_first_line(int fd, const std::string &line) {
    // Reposition the file offset to the beginning of the file.
    if (lseek(fd, 0, SEEK_SET) < 0) {
        perror("lseek");
        return false;
    }
    
    // Ensure the line ends with a newline character.
    std::string output = line;
    if (output.empty() || output.back() != '\n') {
        output.push_back('\n');
    }
    
    // Write the prepared string to the file.
    ssize_t bytesWritten = write(fd, output.c_str(), output.size());
    if (bytesWritten != static_cast<ssize_t>(output.size())) {
        perror("write");
        return false;
    }
    
    // Truncate the file to remove any leftover data beyond what we just wrote.
    if (ftruncate(fd, bytesWritten) < 0) {
        perror("ftruncate");
        return false;
    }
    
    return true;
}

/**
 * Reads and returns the first line from the file referred by the file descriptor.
 *
 * @param fd An open file descriptor.
 * @return   A std::string containing the first line of the file (without the trailing newline).
 */
std::string read_first_line(int fd) {
    // Reposition the file offset to the beginning of the file.
    if (lseek(fd, 0, SEEK_SET) < 0) {
        perror("lseek");
        return "";
    }
    
    std::string firstLine;
    char ch;
    
    // Read one character at a time until newline or end-of-file.
    while (true) {
        ssize_t bytesRead = read(fd, &ch, 1);
        
        if (bytesRead < 0) {
            // An error occurred.
            perror("read");
            return "";
        } else if (bytesRead == 0) {
            // End-of-file reached.
            break;
        }
        
        if (ch == '\n') {
            // Newline found: Stop reading further.
            break;
        }
        firstLine.push_back(ch);
    }
    
    return firstLine;
}

/** Example usage of the functions */
int main() {
    // Open (or create) the file "example.txt" for reading and writing.
    int fd = open("example.txt", O_RDWR | O_CREAT, 0666);
    if (fd < 0) {
        perror("open");
        return 1;
    }
    
    // Write a line to the file.
    if (!write_first_line(fd, "Hello, world!")) {
        std::cerr << "Failed to write the first line." << std::endl;
        close(fd);
        return 1;
    }
    
    // Read the first line back.
    std::string line = read_first_line(fd);
    std::cout << "The first line is: " << line << std::endl;
    
    // Always close the file descriptor when done.
    close(fd);
    return 0;
}
