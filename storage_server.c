/*
 * storage_server.c - Task 25 (Final Core Task)
 *
 * CRITICAL FIX (FAQ Q25): Implement robust, streaming SS initialization.
 * 1. Removed scan_directory_for_files() helper.
 * 2. register_with_name_server() is now a streaming loop:
 * - Sends IP/Port, waits for "ACK_REG".
 * - opendir()s and loops:
 * - send(filename)
 * - recv("ACK_FILE")
 * - After loop, send("SS_FILE_LIST_COMPLETE")
 */

#include <stdio.h>      // For printf, perror, sprintf, fopen, fclose, remove, fread, fgets, rename
#include <stdlib.h>     // For exit(), atoi(), system()
#include <string.h>     // For memset(), strlen(), strncmp(), strtok(), strcpy()
#include <unistd.h>     // For close(), usleep()
#include <arpa/inet.h>  // For inet_pton(), htons()
#include <sys/socket.h> // For all socket functions
#include <pthread.h>    // For Pthreads
#include <stdint.h>     // For intptr_t
#include <sys/stat.h>   // For file stats (getting file size)
#include <time.h>       // For time()
#include <dirent.h>     // For directory scanning
#include <ctype.h>

// --- Name Server Details ---
#define NM_IP "127.0.0.1"
#define NM_PORT 8080     

// --- This Storage Server's Details ---
#define SS_IP "127.0.0.1" 
#define SS_PORT 9091
#define BACKLOG 10        

#define BUFFER_SIZE 1024
#define MAX_CLIENTS 100 
#define MAX_FILE_SIZE 1048576 
#define SS_LOG_FILE "ss.log" 
#define SS_FILE_LIST_COMPLETE "__SS_FILES_DONE__" // NEW: Token for streaming

// --- Global Locking ---
typedef struct {
    char filename[BUFFER_SIZE];
    int sentence_num;
} SentenceLock;

SentenceLock locked_sentences[MAX_CLIENTS];
int lock_count = 0;
pthread_mutex_t lock_list_mutex; 
pthread_mutex_t log_mutex; 

// --- Helper Function Prototype ---
int modify_file(const char* tmp_filename, int target_sentence, const char* write_op);
void log_message(char* message);
// void scan_directory_for_files(char* file_list_buffer, int max_size); // REMOVED


/*
 * Helper: Thread-safe logging function.
 */
void log_message(char* message) {
// ... (This function is unchanged from Task 24) ...
    pthread_mutex_lock(&log_mutex);
    
    FILE *log_file = fopen(SS_LOG_FILE, "a");
    if (log_file == NULL) {
        perror("Failed to open log file");
        pthread_mutex_unlock(&log_mutex);
        return;
    }
    
    char time_str[64];
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    strftime(time_str, sizeof(time_str) - 1, "%Y-%m-%d %H:%M:%S", t);
    
    fprintf(log_file, "[%s] %s\n", time_str, message);
    
    fclose(log_file);
    
    pthread_mutex_unlock(&log_mutex);
}


/*
 * Helper: Modifying the file.
 */
int modify_file(const char* tmp_filename, int target_sentence, const char* write_op) {
    FILE* file = fopen(tmp_filename, "r"); // Read from .tmp
    if (file == NULL) {
        perror("modify_file: fopen (read) failed");
        return -1;
    }

    // 1. Read entire file into memory
    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);
    fseek(file, 0, SEEK_SET);

    if (file_size > MAX_FILE_SIZE) {
        printf("modify_file: File too large to edit in memory.\n");
        fclose(file);
        return -1;
    }

    char* file_content = malloc(file_size + 1);
    if (file_content == NULL) {
        perror("modify_file: malloc failed");
        fclose(file);
        return -1;
    }

    fread(file_content, 1, file_size, file);
    file_content[file_size] = '\0';
    fclose(file);

    // 2. Parse the write operation (safe)
    char write_op_copy[BUFFER_SIZE];
    strncpy(write_op_copy, write_op, BUFFER_SIZE - 1);
    write_op_copy[BUFFER_SIZE - 1] = '\0';

    char* idx_str = strtok(write_op_copy, " ");
    if (idx_str == NULL) {
        printf("modify_file: Invalid write op format (missing index).\n");
        free(file_content);
        return -1;
    }
    char* content_part = strchr(write_op, ' ');
    if (content_part == NULL) {
        printf("modify_file: Invalid write op format (missing content).\n");
        free(file_content);
        return -1;
    }
    content_part++; // points to content after first space

    int target_word = atoi(idx_str);
    if (target_word < 0) {
        printf("modify_file: Invalid word index.\n");
        free(file_content);
        return -1;
    }

    // 3. Find the target sentence. Treat EOF as a valid boundary (allow appending).
    char* file_ptr = file_content;
    int current_sentence = 0;
    char* next_delim = NULL;
    while (current_sentence < target_sentence) {
        next_delim = strpbrk(file_ptr, ".!?");
        if (next_delim == NULL) {
            // No more sentence delimiters: consider the rest of the file a sentence and allow append
            file_ptr = file_content + file_size; // point to EOF
            break;
        }
        file_ptr = next_delim + 1;
        current_sentence++;
    }
    char* sentence_start = file_ptr;
    if (sentence_start > file_content + file_size) sentence_start = file_content + file_size;

    // 4. Find end of the sentence (or EOF)
    char* sentence_end = strpbrk(sentence_start, ".!?");
    if (sentence_end == NULL) sentence_end = file_content + file_size;

    // 5. Find insertion point by word index within sentence_start..sentence_end
    char* p = sentence_start;
    int word_idx = 0;

    // Skip leading whitespace
    while (p < sentence_end && (*p == ' ' || *p == '\t' || *p == '\n')) p++;

    while (word_idx < target_word && p < sentence_end) {
        // advance to next whitespace
        char* next_space = strpbrk(p, " \t\n");
        if (next_space == NULL || next_space >= sentence_end) {
            // reached end of sentence before reaching desired word index
            p = sentence_end;
            break;
        }
        // skip all contiguous spaces to next word
        p = next_space;
        while (p < sentence_end && (*p == ' ' || *p == '\t' || *p == '\n')) p++;
        word_idx++;
    }

    char* insert_pos = p; // where to insert content_part

    // 6. Prepare new file content
    size_t content_len = strlen(content_part);
    // Add one extra space if needed
    int need_space_before = 0;
    if (insert_pos > file_content && insert_pos > sentence_start) {
        char prev_ch = *(insert_pos - 1);
        if (prev_ch != ' ' && prev_ch != '\t' && prev_ch != '\n') need_space_before = 1;
    }
    int need_space_after = 1; // keep a space after inserted content (unless we insert at EOF and content ends with punctuation)
    if (content_len > 0) {
        char lastc = content_part[content_len - 1];
        if (lastc == '.' || lastc == '!' || lastc == '?') need_space_after = 0;
    }

    size_t new_file_size = file_size + content_len + (need_space_before ? 1 : 0) + (need_space_after ? 1 : 0);
    char* new_content = malloc(new_file_size + 1);
    if (new_content == NULL) {
        perror("modify_file: malloc failed for new_content");
        free(file_content);
        return -1;
    }

    // Copy part before insertion
    size_t prefix_len = (size_t)(insert_pos - file_content);
    memcpy(new_content, file_content, prefix_len);

    size_t offset = prefix_len;
    if (need_space_before) {
        new_content[offset++] = ' ';
    }

    // copy inserted content
    memcpy(new_content + offset, content_part, content_len);
    offset += content_len;

    if (need_space_after) {
        // only add space if the next character isn't punctuation and we're not at EOF
        if (insert_pos < file_content + file_size) {
            new_content[offset++] = ' ';
        } else {
            // if appending at EOF and content doesn't end with punctuation, append a space for readability
            new_content[offset++] = ' ';
        }
    }

    // Copy rest of original file after insertion point
    memcpy(new_content + offset, insert_pos, (size_t)(file_size - prefix_len));
    offset += (size_t)(file_size - prefix_len);

    new_content[offset] = '\0';

    // 7. Write new content back to the tmp file
    FILE* out = fopen(tmp_filename, "w");
    if (out == NULL) {
        perror("modify_file: fopen (write) failed");
        free(file_content);
        free(new_content);
        return -1;
    }
    fputs(new_content, out);
    fclose(out);

    // 8. Count new sentences added (approx: count punctuation chars added within content_part)
    int new_sentences = 0;
    for (size_t i = 0; i < content_len; i++) {
        if (content_part[i] == '.' || content_part[i] == '!' || content_part[i] == '?') new_sentences++;
    }

    free(file_content);
    free(new_content);

    return new_sentences >= 0 ? new_sentences : 0;
}

/* * Helper: Updates Name Server with new file stats after a write 
 */
void update_nm_metadata(char* filename) {
    FILE *f = fopen(filename, "r");
    if (!f) return;

    long char_count = 0;
    long word_count = 0;
    int in_word = 0;
    int c;

    while ((c = fgetc(f)) != EOF) {
        char_count++;
        if (c == ' ' || c == '\n' || c == '\t' || c == '\r') {
            in_word = 0;
        } else {
            if (!in_word) {
                word_count++;
                in_word = 1;
            }
        }
    }
    long file_size = ftell(f); // Valid because we read to EOF
    fclose(f);

    // Connect to NM
    int nm_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (nm_sock < 0) return;

    struct sockaddr_in nm_addr;
    memset(&nm_addr, 0, sizeof(nm_addr));
    nm_addr.sin_family = AF_INET;
    nm_addr.sin_port = htons(NM_PORT);
    if (inet_pton(AF_INET, NM_IP, &nm_addr.sin_addr) <= 0) {
        close(nm_sock);
        return;
    }

    if (connect(nm_sock, (struct sockaddr *)&nm_addr, sizeof(nm_addr)) < 0) {
        close(nm_sock);
        return;
    }

    // Send Update Command: UPDATE_META <filename> <size> <words> <chars>
    char update_cmd[BUFFER_SIZE];
    sprintf(update_cmd, "UPDATE_META %s %ld %ld %ld", filename, file_size, word_count, char_count);
    
    send(nm_sock, update_cmd, strlen(update_cmd), 0);
    
    // We don't strictly need to wait for a response here for this async update
    close(nm_sock);
    
    char log_msg[BUFFER_SIZE];
    sprintf(log_msg, "SS-Update: Sent metadata update for %s to NM.", filename);
    log_message(log_msg);
}
/*
 * Thread handler for all connections (UC or NM)
 */
void *handle_connection(void *arg) {
// ... (This function is unchanged from Task 24) ...
    int client_socket = (intptr_t)arg;
    char buffer[BUFFER_SIZE];
    char log_buf[BUFFER_SIZE + 100];
    
    log_message("SS-Handler: Thread started for new connection.");
    printf("SS-Handler: Thread started for new connection.\n");

    memset(buffer, 0, BUFFER_SIZE);
    int bytes_received = recv(client_socket, buffer, BUFFER_SIZE - 1, 0);

    if (bytes_received <= 0) {
        log_message("SS-Handler: Client disconnected or recv error.");
        printf("SS-Handler: Client disconnected or recv error.\n");
        close(client_socket);
        return NULL;
    }

    sprintf(log_buf, "SS-Handler: Received command: '%s'", buffer);
    log_message(log_buf);
    printf("SS-Handler: Received command: '%s'\n", buffer);

    char buffer_copy[BUFFER_SIZE];
    strncpy(buffer_copy, buffer, BUFFER_SIZE);

    char *command = strtok(buffer_copy, " ");
    char *args = NULL;
    if (command != NULL) {
        args = command + strlen(command) + 1;
    }

    if (command == NULL || (args == NULL && strcmp(command, "REQUEST_WRITE") != 0) || (args != NULL && strlen(args) == 0)) {
        if(args == NULL && strcmp(command, "REQUEST_WRITE") == 0) {
        } else {
            const char *err_msg = "ERROR_MALFORMED_COMMAND (Missing command or args)";
            send(client_socket, err_msg, strlen(err_msg), 0);
            log_message("SS-Handler: Sent ERROR_MALFORMED_COMMAND.");
            close(client_socket);
            return NULL;
        }
    }
    
    if (strcmp(command, "CREATE") == 0) {
        sprintf(log_buf, "SS-Handler: Creating file '%s'", args);
        log_message(log_buf);
        printf("SS-Handler: Creating file '%s'\n", args);
        FILE *file = fopen(args, "w"); 
        if (file == NULL) {
            perror("SS-Handler: fopen failed");
            send(client_socket, "ERROR_CREATE_FAILED", 19, 0);
            log_message("SS-Handler: Sent ERROR_CREATE_FAILED.");
        } else {
            fclose(file);
            send(client_socket, "ACK_CREATE_OK", 13, 0);
            log_message("SS-Handler: Sent ACK_CREATE_OK.");
        }
        close(client_socket); 
        return NULL;

    } else if (strcmp(command, "DELETE") == 0) {
        sprintf(log_buf, "SS-Handler: Deleting file '%s'", args);
        log_message(log_buf);
        printf("SS-Handler: Deleting file '%s'\n", args);
        if (remove(args) == 0) {
            send(client_socket, "ACK_DELETE_OK", 13, 0);
            log_message("SS-Handler: Sent ACK_DELETE_OK.");
        } else {
            perror("SS-Handler: remove failed");
            send(client_socket, "ERROR_DELETE_FAILED", 19, 0);
            log_message("SS-Handler: Sent ERROR_DELETE_FAILED.");
        }
        close(client_socket); 
        return NULL;
    
    } else if (strcmp(command, "REQUEST_READ") == 0) {
        sprintf(log_buf, "SS-Handler: Received READ request for '%s'", args);
        log_message(log_buf);
        printf("SS-Handler: Received READ request for '%s'\n", args);
        FILE *file = fopen(args, "rb"); 
        if (file == NULL) {
            perror("SS-Handler: fopen failed");
            send(client_socket, "ERROR_FILE_NOT_FOUND", 20, 0);
            log_message("SS-Handler: Sent ERROR_FILE_NOT_FOUND.");
        } else {
            char file_chunk[BUFFER_SIZE];
            size_t bytes_read;
            while ((bytes_read = fread(file_chunk, 1, BUFFER_SIZE, file)) > 0) {
                if (send(client_socket, file_chunk, bytes_read, 0) < 0) {
                    perror("SS-Handler: send failed during file transfer");
                    log_message("SS-Handler: Send failed during file transfer.");
                    break; 
                }
            }
            fclose(file);
            sprintf(log_buf, "SS-Handler: Finished sending file '%s'.", args);
            log_message(log_buf);
            printf("SS-Handler: Finished sending file '%s'.\n", args);
        }
        close(client_socket); 
        return NULL;
    
    //advance
    } else if (strcmp(command, "SS_COPY") == 0) {
        // Args: SS_COPY <source_file> <dest_file>
        char* src = strtok(args, " ");
        char* dest = strtok(NULL, " ");
        
        if (src && dest) {
            sprintf(log_buf, "SS-Handler: Copying %s to %s", src, dest);
            log_message(log_buf);
            
            // Simple binary copy
            FILE *s = fopen(src, "rb");
            if (s) {
                FILE *d = fopen(dest, "wb");
                if (d) {
                    char buf[4096];
                    size_t n;
                    while ((n = fread(buf, 1, sizeof(buf), s)) > 0) {
                        fwrite(buf, 1, n, d);
                    }
                    fclose(d);
                    send(client_socket, "ACK_COPY", 8, 0);
                } else {
                    perror("SS: dest fopen failed");
                    send(client_socket, "ERROR_COPY", 10, 0);
                }
                fclose(s);
            } else {
                perror("SS: src fopen failed");
                send(client_socket, "ERROR_COPY", 10, 0);
            }
        } else {
             send(client_socket, "ERROR_ARGS", 10, 0);
        }
        close(client_socket);
        return NULL;
        //advance    
    } else if (strcmp(command, "REQUEST_READ_FOR_NM") == 0) {
        sprintf(log_buf, "SS-Handler: Received READ (for NM) request for '%s'", args);
        log_message(log_buf);
        printf("SS-Handler: Received READ (for NM) request for '%s'\n", args);
        FILE *file = fopen(args, "rb"); 
        
        if (file == NULL) {
            perror("SS-Handler: fopen failed");
            send(client_socket, "ERROR_FILE_NOT_FOUND", 20, 0);
            log_message("SS-Handler: Sent ERROR_FILE_NOT_FOUND to NM.");
        } else {
            char file_chunk[BUFFER_SIZE];
            size_t bytes_read;
            while ((bytes_read = fread(file_chunk, 1, BUFFER_SIZE, file)) > 0) {
                if (send(client_socket, file_chunk, bytes_read, 0) < 0) {
                    perror("SS-Handler: send failed during file transfer");
                    log_message("SS-Handler: Send failed during file transfer to NM.");
                    break; 
                }
            }
            fclose(file);
            sprintf(log_buf, "SS-Handler: Finished sending file '%s' to NM.", args);
            log_message(log_buf);
            printf("SS-Handler: Finished sending file '%s' to NM.\n", args);
        }
        close(client_socket); 
        return NULL;
    
    } else if (strcmp(command, "REQUEST_STREAM") == 0) {
        sprintf(log_buf, "SS-Handler: Received STREAM request for '%s'", args);
        log_message(log_buf);
        printf("SS-Handler: Received STREAM request for '%s'\n", args);
        FILE *file = fopen(args, "r"); 
        if (file == NULL) {
            perror("SS-Handler: fopen failed");
            send(client_socket, "ERROR_FILE_NOT_FOUND", 20, 0);
            log_message("SS-Handler: Sent ERROR_FILE_NOT_FOUND.");
        } else {
            char line_buffer[BUFFER_SIZE];
            char *word;
            while (fgets(line_buffer, BUFFER_SIZE, file) != NULL) {
                word = strtok(line_buffer, " \t\n");
                while (word != NULL) {
                    if (send(client_socket, word, strlen(word), 0) < 0) break;
                    if (send(client_socket, " ", 1, 0) < 0) break;
                    usleep(100000); 
                    word = strtok(NULL, " \t\n");
                }
            }
            fclose(file);
            sprintf(log_buf, "SS-Handler: Finished streaming file '%s'.", args);
            log_message(log_buf);
            printf("SS-Handler: Finished streaming file '%s'.\n", args);
        }
        close(client_socket); 
        return NULL;

    } else if (strcmp(command, "UNDO") == 0) {
        sprintf(log_buf, "SS-Handler: Received UNDO request for '%s'", args);
        log_message(log_buf);
        printf("SS-Handler: Received UNDO request for '%s'\n", args);
        
        char bak_filename[BUFFER_SIZE + 5];
        sprintf(bak_filename, "%s.bak", args);

        if (rename(bak_filename, args) == 0) {
            sprintf(log_buf, "SS-Handler: File '%s' restored from backup.", args);
            log_message(log_buf);
            printf("SS-Handler: File '%s' restored from backup.\n", args);
            send(client_socket, "ACK_UNDO_OK", 11, 0);
        } else {
            perror("SS-Handler: rename (undo) failed");
            log_message("SS-Handler: Rename (undo) failed.");
            send(client_socket, "ERROR_NO_UNDO_FILE", 18, 0);
        }
        close(client_socket);
        return NULL;

    } else if (strcmp(command, "CHECK_FILE_LOCKED") == 0) {
// ... (This block is unchanged from Task 24) ...
        sprintf(log_buf, "SS-Handler: Received CHECK_FILE_LOCKED for '%s'", args);
        log_message(log_buf);
        
        pthread_mutex_lock(&lock_list_mutex);
        int found_lock = 0;
        for (int i = 0; i < lock_count; i++) {
            if (strcmp(locked_sentences[i].filename, args) == 0) {
                found_lock = 1;
                break;
            }
        }
        pthread_mutex_unlock(&lock_list_mutex);

        if (found_lock) {
            send(client_socket, "LOCKED", 6, 0);
            log_message("SS-Handler: Sent LOCKED response.");
        } else {
            send(client_socket, "UNLOCKED", 8, 0);
            log_message("SS-Handler: Sent UNLOCKED response.");
        }
        close(client_socket);
        return NULL;
    
    } else if (strcmp(command, "REQUEST_WRITE") == 0) {
// ... (This block is unchanged from Task 24) ...
        char* filename = strtok(args, " ");
        char* sentence_num_str = strtok(NULL, " ");
        if (filename == NULL || sentence_num_str == NULL) {
            send(client_socket, "ERROR_MALFORMED_WRITE", 21, 0);
            log_message("SS-Handler: Sent ERROR_MALFORMED_WRITE.");
            close(client_socket);
            return NULL;
        }
        int sentence_num = atoi(sentence_num_str);
        int original_sentence_num = sentence_num; 
        int write_committed = 0; 

        char tmp_filename[BUFFER_SIZE + 5];
        char bak_filename[BUFFER_SIZE + 5];
        char cp_command[BUFFER_SIZE * 2 + 10];
        
        sprintf(tmp_filename, "%s.tmp", filename);
        sprintf(bak_filename, "%s.bak", filename);

        pthread_mutex_lock(&lock_list_mutex);
        int locked = 0;
        for (int i = 0; i < lock_count; i++) {
            if (strcmp(locked_sentences[i].filename, filename) == 0 &&
                locked_sentences[i].sentence_num == sentence_num) {
                locked = 1;
                break;
            }
        }

        if (locked) {
            pthread_mutex_unlock(&lock_list_mutex);
            send(client_socket, "ERROR_SENTENCE_LOCKED", 21, 0);
            log_message("SS-Handler: Sent ERROR_SENTENCE_LOCKED.");
            close(client_socket);
            return NULL;
        }
        
        if (lock_count >= MAX_CLIENTS) {
            pthread_mutex_unlock(&lock_list_mutex);
            send(client_socket, "ERROR_SERVER_BUSY", 17, 0);
            log_message("SS-Handler: Sent ERROR_SERVER_BUSY (lock list full).");
            close(client_socket);
            return NULL;
        }
        
        strncpy(locked_sentences[lock_count].filename, filename, BUFFER_SIZE);
        locked_sentences[lock_count].sentence_num = sentence_num;
        lock_count++;
        pthread_mutex_unlock(&lock_list_mutex);
        
        sprintf(log_buf, "SS-Handler: Acquired lock for %s, sentence %d", filename, sentence_num);
        log_message(log_buf);
        printf("SS-Handler: Acquired lock for %s, sentence %d\n", filename, sentence_num);

        sprintf(cp_command, "cp %s %s", filename, tmp_filename);
        if (system(cp_command) != 0) {
             log_message("SS-Handler: CRITICAL - Failed to create .tmp file for WRITE.");
             printf("SS-Handler: CRITICAL - Failed to create .tmp file for WRITE.\n");
             send(client_socket, "ERROR_WRITE_SETUP_FAILED", 24, 0);
        } else {
            sprintf(log_buf, "SS-Handler: Created temp write file at %s", tmp_filename);
            log_message(log_buf);
            printf("SS-Handler: Created temp write file at %s\n", tmp_filename);
            
            send(client_socket, "ACK_LOCKED", 10, 0);
            log_message("SS-Handler: Sent ACK_LOCKED.");
            
            while(1) {
                memset(buffer, 0, BUFFER_SIZE);
                bytes_received = recv(client_socket, buffer, BUFFER_SIZE - 1, 0);

                if (bytes_received <= 0) {
                    log_message("SS-Handler: Client disconnected during write.");
                    printf("SS-Handler: Client disconnected during write.\n");
                    break; 
                }
                
                sprintf(log_buf, "SS-Handler: Received write op: %s", buffer);
                log_message(log_buf);

                if (strncmp(buffer, "ETIRW", 5) == 0) {
                    if (rename(filename, bak_filename) != 0) {
                         perror("SS-Handler: rename to .bak failed");
                         log_message("SS-Handler: ERROR - rename to .bak failed.");
                         send(client_socket, "ERROR_COMMIT_FAILED", 19, 0);
                         break; 
                    }
                    
                    if (rename(tmp_filename, filename) != 0) {
                        perror("SS-Handler: rename from .tmp failed");
                        log_message("SS-Handler: ERROR - rename from .tmp failed.");
                        rename(bak_filename, filename); 
                        send(client_socket, "ERROR_COMMIT_FAILED", 19, 0);
                        break; 
                    }
                    
                    write_committed = 1;
                    printf("SS-Handler: Received ETIRW. Write committed.\n");
                    send(client_socket, "ACK_WRITE_COMPLETE", 18, 0);
                    log_message("SS-Handler: Sent ACK_WRITE_COMPLETE.");
                    update_nm_metadata(filename);
                    break; 
                
                } else {
                    int new_sentences = modify_file(tmp_filename, sentence_num, buffer); 
                    
                    if (new_sentences >= 0) {
                        send(client_socket, "ACK_UPDATED", 11, 0);
                        log_message("SS-Handler: Sent ACK_UPDATED.");
                        sentence_num += new_sentences; 
                    } else {
                        send(client_socket, "ERROR_WRITE_FAILED", 18, 0);
                        log_message("SS-Handler: Sent ERROR_WRITE_FAILED.");
                    }
                }
            } // end while(1)
        }
        
        if (!write_committed) {
            remove(tmp_filename);
            log_message("SS-Handler: Write not committed. Discarding .tmp file.");
        }
        
        pthread_mutex_lock(&lock_list_mutex);
        int lock_index = -1;
        for (int i = 0; i < lock_count; i++) {
             if (strcmp(locked_sentences[i].filename, filename) == 0 &&
                 locked_sentences[i].sentence_num == original_sentence_num) {
                lock_index = i;
                break;
            }
        }
        if (lock_index != -1) {
            for (int i = lock_index; i < lock_count - 1; i++) {
                locked_sentences[i] = locked_sentences[i+1];
            }
            lock_count--;
            sprintf(log_buf, "SS-Handler: Releasing lock for %s, sentence %d", filename, original_sentence_num);
            log_message(log_buf);
            printf("SS-Handler: Releasing lock for %s, sentence %d\n", filename, original_sentence_num);
        }
        pthread_mutex_unlock(&lock_list_mutex);
        
        close(client_socket);
        return NULL;
        
     //advance
     } else if (strcmp(command, "SS_SAVE") == 0) {
        // Format: SS_SAVE <filename> (Data sent after newline)
        // Used by NM to push replicated data
        if (!args) { send(client_socket, "ERROR_ARGS", 10, 0); return NULL; }
        
        sprintf(log_buf, "SS-Handler: Saving replicated file '%s'", args);
        log_message(log_buf);
        
        FILE *f = fopen(args, "wb");
        if (f) {
            send(client_socket, "READY_TO_RECEIVE", 16, 0); // Signal NM to start sending
            
            char recv_buf[BUFFER_SIZE];
            int n;
            while ((n = recv(client_socket, recv_buf, BUFFER_SIZE, 0)) > 0) {
                fwrite(recv_buf, 1, n, f);
            }
            fclose(f);
            log_message("SS-Handler: Replication save complete.");
        } else {
            perror("SS: fopen failed for save");
        }
        close(client_socket);
        return NULL;
        //advance   
    } else {
        printf("SS-Handler: Unknown command received.\n");
        send(client_socket, "ERROR_UNKNOWN_SS_COMMAND", 24, 0);
        log_message("SS-Handler: Sent ERROR_UNKNOWN_SS_COMMAND.");
        close(client_socket);
        return NULL;
    }
}


/*
 * Registers this SS with the Name Server
 * PATCHED for streaming file list
 */
void register_with_name_server() {
    int nm_sock_fd;
    struct sockaddr_in nm_addr;
    char reg_msg[BUFFER_SIZE];
    char log_buf[BUFFER_SIZE + 100];
    char ack_buffer[BUFFER_SIZE];

    sprintf(reg_msg, "SS %s %d", SS_IP, SS_PORT);

    nm_sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (nm_sock_fd < 0) {
        perror("SS-Main: NM socket failed");
        log_message("SS-Main: NM socket failed.");
        exit(EXIT_FAILURE);
    }

    memset(&nm_addr, 0, sizeof(nm_addr));
    nm_addr.sin_family = AF_INET;
    nm_addr.sin_port = htons(NM_PORT);
    if (inet_pton(AF_INET, NM_IP, &nm_addr.sin_addr) <= 0) {
        perror("SS-Main: NM invalid address");
        log_message("SS-Main: NM invalid address.");
        close(nm_sock_fd);
        exit(EXIT_FAILURE);
    }

    if (connect(nm_sock_fd, (struct sockaddr *)&nm_addr, sizeof(nm_addr)) < 0) {
        perror("SS-Main: NM connect failed");
        log_message("SS-Main: NM connect failed.");
        close(nm_sock_fd);
        exit(EXIT_FAILURE);
    }

    // --- Step 1: Send Registration ---
    if (send(nm_sock_fd, reg_msg, strlen(reg_msg), 0) < 0) {
        perror("SS-Main: NM send failed");
        log_message("SS-Main: NM send failed.");
        close(nm_sock_fd);
        exit(EXIT_FAILURE);
    }

    // --- Step 2: Wait for ACK_REG ---
    memset(ack_buffer, 0, BUFFER_SIZE);
    if (recv(nm_sock_fd, ack_buffer, BUFFER_SIZE - 1, 0) <= 0) {
        perror("SS-Main: NM ACK recv failed");
        log_message("SS-Main: NM did not send ACK_REG.");
        close(nm_sock_fd);
        exit(EXIT_FAILURE);
    }

    if (strncmp(ack_buffer, "ACK_REG", 7) != 0) {
        log_message("SS-Main: Received invalid ACK from NM.");
        printf("SS-Main: Received invalid ACK from NM: %s\n", ack_buffer);
        close(nm_sock_fd);
        exit(EXIT_FAILURE);
    }

    // --- Step 3: Scan directory and stream file list ---
    log_message("SS-Main: NM acknowledged. Scanning directory and streaming file list...");
    printf("SS-Main: NM acknowledged. Scanning directory and streaming file list...\n");
    
    DIR *d = opendir(".");
    if (d) {
        struct dirent *dir;
        char send_buf[BUFFER_SIZE];
        char ack_buffer[BUFFER_SIZE];
        while ((dir = readdir(d)) != NULL) {
            if (dir->d_type == DT_REG) {
                char *dot = strrchr(dir->d_name, '.');
                if (dot && strcmp(dot, ".txt") == 0) {
                    const char *fname = dir->d_name;
                    struct stat st;
                    long fsize = 0;
                    long mtime = time(NULL);
                    if (stat(fname, &st) == 0) {
                        fsize = (long)st.st_size;
                        mtime = (long)st.st_mtime;
                    }

                    // count words and chars
                    long word_count = 0;
                    long char_count = 0;
                    FILE *f = fopen(fname, "rb");
                    if (f) {
                        int c;
                        int in_word = 0;
                        while ((c = fgetc(f)) != EOF) {
                            char_count++;
                            if (isspace((unsigned char)c)) {
                                in_word = 0;
                            } else {
                                if (!in_word) { word_count++; in_word = 1; }
                            }
                        }
                        fclose(f);
                    }

                    snprintf(send_buf, sizeof(send_buf), "%s|%ld|%ld|%ld|%ld",
                             fname, fsize, mtime, word_count, char_count);
                    if (send(nm_sock_fd, send_buf, strlen(send_buf), 0) < 0) {
                        log_message("SS-Main: Failed to send filename+meta. Aborting list.");
                        break;
                    }

                    memset(ack_buffer, 0, sizeof(ack_buffer));
                    if (recv(nm_sock_fd, ack_buffer, sizeof(ack_buffer) - 1, 0) <= 0) {
                        log_message("SS-Main: NM disconnected during file list stream.");
                        break;
                    }
                    // expect "ACK_FILE"
                }
            }
        }
        closedir(d);
        send(nm_sock_fd, SS_FILE_LIST_COMPLETE, strlen(SS_FILE_LIST_COMPLETE), 0);
    } else {
        perror("SS-Main: opendir failed");
        log_message("SS-Main: Failed to open directory to scan files.");
    }
    
    sprintf(log_buf, "SS-Main: Registered with Name Server as '%s' and streamed file list.", reg_msg);
    log_message(log_buf);
    printf("SS-Main: Registered with Name Server as '%s' and streamed file list.\n", reg_msg);
    close(nm_sock_fd);
}

int main() {
// ... (This function is unchanged from Task 24) ...
    if (pthread_mutex_init(&lock_list_mutex, NULL) != 0) {
        perror("SS-Main: lock_mutex_init failed");
        exit(EXIT_FAILURE);
    }
    if (pthread_mutex_init(&log_mutex, NULL) != 0) {
        perror("SS-Main: log_mutex_init failed");
        exit(EXIT_FAILURE);
    }
    
    register_with_name_server();


    int ss_listen_fd, new_conn_fd;
    struct sockaddr_in ss_addr, new_client_addr;

    ss_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (ss_listen_fd < 0) {
        perror("SS-Main: Server socket failed");
        log_message("SS-Main: Server socket failed.");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    if (setsockopt(ss_listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("SS-Main: setsockopt failed");
        log_message("SS-Main: setsockopt failed.");
        close(ss_listen_fd);
        exit(EXIT_FAILURE);
    }

    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_port = htons(SS_PORT);
    ss_addr.sin_addr.s_addr = INADDR_ANY; 

    if (bind(ss_listen_fd, (struct sockaddr *)&ss_addr, sizeof(ss_addr)) < 0) {
        perror("SS-Main: bind failed");
        log_message("SS-Main: bind failed.");
        close(ss_listen_fd);
        exit(EXIT_FAILURE);
    }

    if (listen(ss_listen_fd, BACKLOG) < 0) {
        perror("SS-Main: listen failed");
        log_message("SS-Main: listen failed.");
        close(ss_listen_fd);
        exit(EXIT_FAILURE);
    }

    char log_buf[100];
    sprintf(log_buf, "SS-Main: Storage Server now listening on port %d...", SS_PORT);
    log_message(log_buf);
    printf("SS-Main: Storage Server now listening on port %d...\n", SS_PORT);

    while (1) {
        socklen_t client_len = sizeof(new_client_addr);
        
        new_conn_fd = accept(ss_listen_fd, (struct sockaddr *)&new_client_addr, &client_len);
        if (new_conn_fd < 0) {
            perror("SS-Main: accept failed");
            log_message("SS-Main: accept failed.");
            continue; 
        }

        sprintf(log_buf, "SS-Main: Accepted new connection from %s:%d",
               inet_ntoa(new_client_addr.sin_addr), ntohs(new_client_addr.sin_port));
        log_message(log_buf);
        printf("SS-Main: Accepted new connection from %s:%d\n",
               inet_ntoa(new_client_addr.sin_addr), ntohs(new_client_addr.sin_port));

        pthread_t thread_id;
        if (pthread_create(&thread_id, NULL, handle_connection, (void*)(intptr_t)new_conn_fd) != 0) {
            perror("SS-Main: pthread_create failed");
            log_message("SS-Main: pthread_create failed.");
            close(new_conn_fd);
        } else {
            pthread_detach(thread_id);
        }
    }

    pthread_mutex_destroy(&lock_list_mutex);
    pthread_mutex_destroy(&log_mutex);
    close(ss_listen_fd);
    return 0;
}