#include <iostream>
#include <string>
#include <openssl/evp.h>
#include <openssl/md5.h>

// Helper function to convert __int128 to a decimal string representation.
std::string int128ToString(__int128 n) {
    if (n == 0)
        return "0";
    std::string result;
    while (n > 0) {
        int digit = n % 10;
        result.insert(result.begin(), '0' + digit);
        n /= 10;
    }
    return result;
}

// Hashes the input string using MD5 via the EVP interface and converts the result to a __int128.
__int128 hashMD5ToInt(const std::string &input) {
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_len = 0;

    // Create and initialize the digest context.
    EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
    if (mdctx == nullptr) {
        std::cerr << "Error: EVP_MD_CTX_new failed" << std::endl;
        return 0;
    }
    
    // Initialize the digest context for MD5.
    if (1 != EVP_DigestInit_ex(mdctx, EVP_md5(), nullptr)) {
        std::cerr << "Error: EVP_DigestInit_ex failed" << std::endl;
        EVP_MD_CTX_free(mdctx);
        return 0;
    }
    
    // Update the context with the input data.
    if (1 != EVP_DigestUpdate(mdctx, input.data(), input.size())) {
        std::cerr << "Error: EVP_DigestUpdate failed" << std::endl;
        EVP_MD_CTX_free(mdctx);
        return 0;
    }
    
    // Finalize the digest and store the result in 'digest'.
    if (1 != EVP_DigestFinal_ex(mdctx, digest, &digest_len)) {
        std::cerr << "Error: EVP_DigestFinal_ex failed" << std::endl;
        EVP_MD_CTX_free(mdctx);
        return 0;
    }
    
    // Clean up the digest context.
    EVP_MD_CTX_free(mdctx);

    // Combine the digest bytes into a __int128.
    __int128 result = 0;
    for (unsigned int i = 0; i < digest_len; i++) {
        result = (result << 8) | digest[i];
    }
    return result;
}

int main() {
    std::string input = "hello";
    __int128 hashValue = hashMD5ToInt(input);
    
    std::cout << "MD5 hash of \"" << input << "\" as integer is:\n"
              << int128ToString(hashValue) << std::endl;
    
    return 0;
}
