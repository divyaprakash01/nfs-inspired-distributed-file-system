/*
 * name_server.c - Task 25 (Final Core Task)
 *
 * CRITICAL FIX (FAQ Q25): Implement robust, streaming SS initialization.
 * 1. handle_client (SS block) is now a while(1) streaming loop.
 * - Receives IP/Port, sends "ACK_REG".
 * - Loops:
 * - recv(filename)
 * - Check for "SS_FILE_LIST_COMPLETE"
 * - Check for duplicates
 * - Add to map if new
 * - send("ACK_FILE")
 * 2. Calls save_metadata_to_disk() *once* at the end.
 */

#include <stdio.h>      // For standard input/output functions (printf, perror, popen, pclose, fgets, FILE, fopen, fprintf, fclose)
#include <stdlib.h>     // For exit(), malloc(), free(), atoi()
#include <string.h>     // For memset(), strstr(), strncmp(), strcspn(), strcat(), strcpy(), strtok()
#include <unistd.h>     // For close()
#include <arpa/inet.h>  // For inet_ntoa(), ntohs(), inet_pton()
#include <sys/socket.h> // For socket(), bind(), listen(), accept(), recv()
#include <pthread.h>    // For Pthreads (mutex and threads)
#include <stdint.h>     // For intptr_t
#include <time.h>       // For time_t, time(), localtime(), strftime()

#define PORT 8080       
#define BUFFER_SIZE 1024
#define RESPONSE_SIZE 2048 
#define MAX_CLIENTS 100 
#define MAX_FILE_SIZE 4096 
#define EXEC_TERMINATOR "__EXEC_COMPLETE__" 
#define NM_LOG_FILE "nm.log" 
#define HASH_MAP_SIZE 100 
#define METADATA_FILE "nm_metadata.dat"
#define UC_DATA_FILE "uc_list.dat" 
#define CACHE_SIZE 10 
#define SS_FILE_LIST_COMPLETE "__SS_FILES_DONE__" // NEW: Token for streaming

// --- Global Data Structures ---
// ... (Structs are unchanged from Task 24) ...
typedef struct {
    char ip[INET_ADDRSTRLEN]; 
    int port;                 
} StorageServer;

typedef struct {
    char username[BUFFER_SIZE];
} UserClient;

typedef struct {
    char username[BUFFER_SIZE];
    char access_type; // 'R' for Read, 'W' for Write
} AccessPermission;

//advance
typedef struct {
    char username[BUFFER_SIZE];
    char request_type; // 'R' for Read, 'W' for Write
} RequestEntry;

typedef struct {
    char filename[BUFFER_SIZE];
    char owner_username[BUFFER_SIZE];
    long file_size;
    time_t creation_time;
    time_t last_modified_time;
    time_t last_accessed_time;
    char last_accessed_by[BUFFER_SIZE];
    StorageServer location;
    AccessPermission access_list[MAX_CLIENTS];
    int access_count;
    long word_count;   // added: number of words (from SS)
    long char_count;   // added: number of characters (from SS)
    int is_directory;               // 1 if folder, 0 if file
    char parent_dir[BUFFER_SIZE];   // Name of parent folder (default "root")
    //advance
    // --- PART 3: REQUESTS ---
    RequestEntry pending_requests[MAX_CLIENTS];
    int request_count;
    // ------------------------
    // --- PART 4: REPLICATION ---
    int has_backup;                 // 1 if replicated
    StorageServer backup_location;  // Location of backup copy
    // ---------------------------
} FileInfo;

typedef struct Node {
    FileInfo file;
    struct Node* next;
} Node;

typedef struct CacheNode {
    char filename[BUFFER_SIZE];
    FileInfo file_data;
    struct CacheNode* next;
    struct CacheNode* prev;
} CacheNode;

CacheNode* lru_cache_head = NULL;
CacheNode* lru_cache_tail = NULL;
int cache_current_size = 0;


// --- Function Prototypes ---
// ... (Prototypes are unchanged from Task 24) ...
int proxy_command_to_ss(const char* ss_ip, int ss_port, const char* command);
int proxy_delete_command_to_ss(const char* ss_ip, int ss_port, const char* command);
int proxy_undo_command_to_ss(const char* ss_ip, int ss_port, const char* command); 
void *handle_client(void *arg);
int check_permission(char* username, FileInfo* file, char required_access);
int fetch_file_content_from_ss(StorageServer ss, const char* filename, char* out_content, int max_size);
void log_message(char* message);
unsigned int hash(char* filename); 
Node* find_file_node(char* filename, unsigned int* out_index);
void load_metadata_from_disk();
void save_metadata_to_disk();
void cache_move_to_front(CacheNode* node); 
void cache_evict(); 
int cache_get(char* filename, FileInfo* out_file); 
void cache_put(FileInfo* file_data); 
void cache_invalidate(char* filename); 
void load_users_from_disk(); 
void save_users_to_disk();
int check_if_file_is_locked_on_ss(StorageServer ss, const char* filename); 


// --- Global Lists & Maps ---
// ... (Global lists are unchanged from Task 24) ...
StorageServer ss_list[MAX_CLIENTS];
UserClient    online_users[MAX_CLIENTS]; 
UserClient    all_users[MAX_CLIENTS];    
int ss_count = 0;
int online_count = 0; 
int all_users_count = 0; 

Node* file_hash_map[HASH_MAP_SIZE];

// --- Global Mutexes ---
// ... (Mutexes are unchanged from Task 24) ...
pthread_mutex_t list_mutex;   
pthread_mutex_t log_mutex;    
pthread_mutex_t map_mutexes[HASH_MAP_SIZE]; 
pthread_mutex_t cache_mutex; 


/*
 * Helper: Simple hash function
 */
unsigned int hash(char* filename) {
// ... (This function is unchanged from Task 24) ...
    unsigned int hash_val = 0;
    for (int i = 0; i < strlen(filename); i++) {
        hash_val += (unsigned int)filename[i];
    }
    return hash_val % HASH_MAP_SIZE;
}

/*
 * Helper: Finds a file node and locks its bucket.
 */
Node* find_file_node(char* filename, unsigned int* out_index) {
// ... (This function is unchanged from Task 24) ...
    *out_index = hash(filename);
    pthread_mutex_lock(&map_mutexes[*out_index]);
    
    Node* current = file_hash_map[*out_index];
    while (current != NULL) {
        if (strcmp(current->file.filename, filename) == 0) {
            return current; 
        }
        current = current->next;
    }
    
    pthread_mutex_unlock(&map_mutexes[*out_index]);
    return NULL;
}


/*
 * Helper: Thread-safe logging function.
 */
void log_message(char* message) {
// ... (This function is unchanged from Task 24) ...
    pthread_mutex_lock(&log_mutex);
    
    FILE *log_file = fopen(NM_LOG_FILE, "a");
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
 * Helper: Loads the file hash map from disk on startup.
 */
void load_metadata_from_disk() {
// ... (This function is unchanged from Task 24) ...
    log_message("Loading metadata from disk...");
    FILE *file = fopen(METADATA_FILE, "rb");
    if (file == NULL) {
        log_message("No metadata file found. Starting fresh.");
        return;
    }

    FileInfo temp_file;
    while (fread(&temp_file, sizeof(FileInfo), 1, file) == 1) {
        Node* new_node = (Node*)malloc(sizeof(Node));
        if (new_node == NULL) {
            log_message("ERROR: malloc failed during metadata load.");
            continue;
        }
        new_node->file = temp_file;
        
        unsigned int index = hash(temp_file.filename);
        new_node->next = file_hash_map[index];
        file_hash_map[index] = new_node;
    }

    fclose(file);
    log_message("Metadata loaded successfully.");
}

/*
 * Helper: Saves the entire file hash map to disk.
 */
void save_metadata_to_disk() {
// ... (This function is unchanged from Task 24) ...
    log_message("Saving metadata to disk...");
    FILE *file = fopen(METADATA_FILE, "wb");
    if (file == NULL) {
        log_message("ERROR: Could not open metadata file for writing!");
        return;
    }

    for (int i = 0; i < HASH_MAP_SIZE; i++) {
        pthread_mutex_lock(&map_mutexes[i]);
    }
    
    for (int i = 0; i < HASH_MAP_SIZE; i++) {
        Node* current = file_hash_map[i];
        while (current != NULL) {
            if (fwrite(&(current->file), sizeof(FileInfo), 1, file) != 1) {
                log_message("ERROR: Failed to write metadata to disk.");
                break;
            }
            current = current->next;
        }
    }

    for (int i = 0; i < HASH_MAP_SIZE; i++) {
        pthread_mutex_unlock(&map_mutexes[i]);
    }
    
    fclose(file);
    log_message("Metadata saved successfully.");
}


/*
 * Helper: Loads the persistent user list from disk.
 */
void load_users_from_disk() {
// ... (This function is unchanged from Task 24) ...
    log_message("Loading user list from disk...");
    FILE *file = fopen(UC_DATA_FILE, "rb");
    if (file == NULL) {
        log_message("No user list file found. Starting fresh.");
        return;
    }

    if (fread(&all_users_count, sizeof(int), 1, file) != 1) {
        log_message("ERROR: Failed to read user count from disk.");
        fclose(file);
        return;
    }

    if (fread(all_users, sizeof(UserClient), all_users_count, file) != (size_t)all_users_count) {
        log_message("ERROR: Failed to read full user list from disk.");
        all_users_count = 0; 
    }

    fclose(file);
    char log_buf[100];
    sprintf(log_buf, "Loaded %d persistent users.", all_users_count);
    log_message(log_buf);
}

/*
 * Helper: Saves the persistent user list to disk.
 */
void save_users_to_disk() {
// ... (This function is unchanged from Task 24) ...
    log_message("Saving user list to disk...");
    FILE *file = fopen(UC_DATA_FILE, "wb");
    if (file == NULL) {
        log_message("ERROR: Could not open user list file for writing!");
        return;
    }

    if (fwrite(&all_users_count, sizeof(int), 1, file) != 1) {
        log_message("ERROR: Failed to write user count to disk.");
        fclose(file);
        return;
    }

    if (fwrite(all_users, sizeof(UserClient), all_users_count, file) != (size_t)all_users_count) {
        log_message("ERROR: Failed to write full user list to disk.");
    }

    fclose(file);
    log_message("User list saved successfully.");
}


/*
 * Cache Helper functions (cache_move_to_front, cache_evict, 
 * cache_get, cache_put, cache_invalidate)
 */
// ... (These 5 functions are unchanged from Task 24) ...
void cache_move_to_front(CacheNode* node) {
    if (node == lru_cache_head) {
        return; 
    }
    if (node->prev) node->prev->next = node->next;
    if (node->next) node->next->prev = node->prev;
    if (node == lru_cache_tail && node->prev) {
        lru_cache_tail = node->prev;
    }
    node->next = lru_cache_head;
    node->prev = NULL;
    if (lru_cache_head) {
        lru_cache_head->prev = node;
    }
    lru_cache_head = node;
    if (lru_cache_tail == NULL) {
        lru_cache_tail = node;
    }
}
void cache_evict() {
    if (lru_cache_tail == NULL) {
        return; 
    }
    CacheNode* temp = lru_cache_tail;
    if (lru_cache_tail->prev) {
        lru_cache_tail = lru_cache_tail->prev;
        lru_cache_tail->next = NULL;
    } else {
        lru_cache_head = NULL;
        lru_cache_tail = NULL;
    }
    free(temp);
    cache_current_size--;
}
int cache_get(char* filename, FileInfo* out_file) {
    pthread_mutex_lock(&cache_mutex);
    CacheNode* current = lru_cache_head;
    while (current != NULL) {
        if (strcmp(current->filename, filename) == 0) {
            *out_file = current->file_data; 
            cache_move_to_front(current);
            pthread_mutex_unlock(&cache_mutex);
            return 1; 
        }
        current = current->next;
    }
    pthread_mutex_unlock(&cache_mutex);
    return 0; 
}
void cache_put(FileInfo* file_data) {
    pthread_mutex_lock(&cache_mutex);
    CacheNode* current = lru_cache_head;
    while (current != NULL) {
        if (strcmp(current->filename, file_data->filename) == 0) {
            current->file_data = *file_data;
            cache_move_to_front(current);
            pthread_mutex_unlock(&cache_mutex);
            return;
        }
        current = current->next;
    }
    if (cache_current_size >= CACHE_SIZE) {
        cache_evict();
    }
    CacheNode* new_node = (CacheNode*)malloc(sizeof(CacheNode));
    if (new_node == NULL) {
        log_message("ERROR: malloc failed for cache node.");
        pthread_mutex_unlock(&cache_mutex);
        return;
    }
    new_node->file_data = *file_data;
    strcpy(new_node->filename, file_data->filename);
    new_node->next = lru_cache_head;
    new_node->prev = NULL;
    if (lru_cache_head) {
        lru_cache_head->prev = new_node;
    }
    lru_cache_head = new_node;
    if (lru_cache_tail == NULL) {
        lru_cache_tail = new_node;
    }
    cache_current_size++;
    pthread_mutex_unlock(&cache_mutex);
}
void cache_invalidate(char* filename) {
    pthread_mutex_lock(&cache_mutex);
    CacheNode* current = lru_cache_head;
    while (current != NULL) {
        if (strcmp(current->filename, filename) == 0) {
            if (current->prev) current->prev->next = current->next;
            if (current->next) current->next->prev = current->prev;
            if (current == lru_cache_head) lru_cache_head = current->next;
            if (current == lru_cache_tail) lru_cache_tail = current->prev;
            free(current);
            cache_current_size--;
            pthread_mutex_unlock(&cache_mutex);
            return; 
        }
        current = current->next;
    }
    pthread_mutex_unlock(&cache_mutex);
}


/*
 * Helper: Checks if a user has the required permission for a file.
 */
int check_permission(char* username, FileInfo* file, char required_access) {
    if (username == NULL || file == NULL) return 0;

    // ensure strings are NUL-terminated before comparing
    file->owner_username[BUFFER_SIZE-1] = '\0';

    if (strncmp(file->owner_username, username, BUFFER_SIZE) == 0) {
        return 1;
    }

    // clamp access_count to reasonable bounds to avoid corrupted metadata loops
    int ac = file->access_count;
    if (ac < 0) ac = 0;
    if (ac > MAX_CLIENTS) ac = MAX_CLIENTS;

    for (int i = 0; i < ac; i++) {
        file->access_list[i].username[BUFFER_SIZE-1] = '\0';
        if (strncmp(file->access_list[i].username, username, BUFFER_SIZE) == 0) {
            char t = file->access_list[i].access_type;
            if (t == 'W') return 1;
            if (required_access == 'R' && t == 'R') return 1;
            return 0;
        }
    }
    return 0;
}


/*
 * Helper: NM acts as client to SS for CREATE
 */
int proxy_command_to_ss(const char* ss_ip, int ss_port, const char* command) {
// ... (This function is unchanged from Task 24) ...
    int ss_sock_fd;
    struct sockaddr_in ss_addr;
    ss_sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (ss_sock_fd < 0) {
        perror("NM-Proxy: socket failed");
        return 0;
    }
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_port = htons(ss_port);
    if (inet_pton(AF_INET, ss_ip, &ss_addr.sin_addr) <= 0) {
        perror("NM-Proxy: invalid address");
        close(ss_sock_fd);
        return 0;
    }
    if (connect(ss_sock_fd, (struct sockaddr *)&ss_addr, sizeof(ss_addr)) < 0) {
        perror("NM-Proxy: connect failed");
        close(ss_sock_fd);
        return 0;
    }
    if (send(ss_sock_fd, command, strlen(command), 0) < 0) {
        perror("NM-Proxy: send failed");
        close(ss_sock_fd);
        return 0;
    }
    char ss_response[BUFFER_SIZE];
    memset(ss_response, 0, BUFFER_SIZE);
    if (recv(ss_sock_fd, ss_response, BUFFER_SIZE - 1, 0) <= 0) {
        perror("NM-Proxy: recv failed");
        close(ss_sock_fd);
        return 0;
    }
    close(ss_sock_fd);
    if (strncmp(ss_response, "ACK_CREATE_OK", 13) == 0) {
        return 1; 
    } else {
        printf("NM-Proxy: SS returned an error: %s\n", ss_response);
        return 0; 
    }
}

/*
 * Helper: NM acts as client to SS for DELETE
 */
int proxy_delete_command_to_ss(const char* ss_ip, int ss_port, const char* command) {
// ... (This function is unchanged from Task 24) ...
    int ss_sock_fd;
    struct sockaddr_in ss_addr;
    ss_sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (ss_sock_fd < 0) {
        perror("NM-Proxy-DELETE: socket failed");
        return 0;
    }
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_port = htons(ss_port);
    if (inet_pton(AF_INET, ss_ip, &ss_addr.sin_addr) <= 0) {
        perror("NM-Proxy-DELETE: invalid address");
        close(ss_sock_fd);
        return 0;
    }
    if (connect(ss_sock_fd, (struct sockaddr *)&ss_addr, sizeof(ss_addr)) < 0) {
        perror("NM-Proxy-DELETE: connect failed");
        close(ss_sock_fd);
        return 0;
    }
   // if (send(ss_sock_fd, command, strlen(command), 0) < 0) {
    //}
    if (send(ss_sock_fd, command, strlen(command), 0) < 0) {
        perror("NM-Proxy-DELETE: send failed");
        close(ss_sock_fd);
        return 0;
    }
    char ss_response[BUFFER_SIZE];
    memset(ss_response, 0, BUFFER_SIZE);
    if (recv(ss_sock_fd, ss_response, BUFFER_SIZE - 1, 0) <= 0) {
        perror("NM-Proxy-DELETE: recv failed");
        close(ss_sock_fd);
        return 0;
    }
    close(ss_sock_fd);
    if (strncmp(ss_response, "ACK_DELETE_OK", 13) == 0) {
        return 1; 
    } else {
        printf("NM-Proxy-DELETE: SS returned an error: %s\n", ss_response);
        return 0; 
    }
}

/*
 * Helper: NM acts as client to SS for UNDO
 */
int proxy_undo_command_to_ss(const char* ss_ip, int ss_port, const char* command) {
// ... (This function is unchanged from Task 24) ...
    int ss_sock_fd;
    struct sockaddr_in ss_addr;
    ss_sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (ss_sock_fd < 0) {
        perror("NM-Proxy-UNDO: socket failed");
        return 0;
    }
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_port = htons(ss_port);
    if (inet_pton(AF_INET, ss_ip, &ss_addr.sin_addr) <= 0) {
        perror("NM-Proxy-UNDO: invalid address");
        close(ss_sock_fd);
        return 0;
    }
    if (connect(ss_sock_fd, (struct sockaddr *)&ss_addr, sizeof(ss_addr)) < 0) {
        perror("NM-Proxy-UNDO: connect failed");
        close(ss_sock_fd);
        return 0;
    }
    if (send(ss_sock_fd, command, strlen(command), 0) < 0) {
        perror("NM-Proxy-UNDO: send failed");
        close(ss_sock_fd);
        return 0;
    }
    char ss_response[BUFFER_SIZE];
    memset(ss_response, 0, BUFFER_SIZE);
    if (recv(ss_sock_fd, ss_response, BUFFER_SIZE - 1, 0) <= 0) {
        perror("NM-Proxy-UNDO: recv failed");
        close(ss_sock_fd);
        return 0;
    }
    close(ss_sock_fd);
    if (strncmp(ss_response, "ACK_UNDO_OK", 11) == 0) {
        return 1; 
    } else {
        printf("NM-Proxy-UNDO: SS returned an error: %s\n", ss_response);
        return 0; 
    }
}

/*
 * Helper: NM acts as client to SS to fetch file content for EXEC.
 */
int fetch_file_content_from_ss(StorageServer ss, const char* filename, char* out_content, int max_size) {
// ... (This function is unchanged from Task 24) ...
    int ss_sock_fd;
    struct sockaddr_in ss_addr;
    char command[BUFFER_SIZE];
    
    sprintf(command, "REQUEST_READ_FOR_NM %s", filename);

    ss_sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (ss_sock_fd < 0) {
        perror("NM-Fetch: socket failed");
        return 0;
    }

    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_port = htons(ss.port);
    if (inet_pton(AF_INET, ss.ip, &ss_addr.sin_addr) <= 0) {
        perror("NM-Fetch: invalid address");
        close(ss_sock_fd);
        return 0;
    }

    if (connect(ss_sock_fd, (struct sockaddr *)&ss_addr, sizeof(ss_addr)) < 0) {
        perror("NM-Fetch: connect failed");
        close(ss_sock_fd);
        return 0;
    }

    if (send(ss_sock_fd, command, strlen(command), 0) < 0) {
        perror("NM-Fetch: send failed");
        close(ss_sock_fd);
        return 0;
    }

    int total_bytes = 0;
    int bytes_received;
    while((bytes_received = recv(ss_sock_fd, out_content + total_bytes, max_size - total_bytes - 1, 0)) > 0) {
        total_bytes += bytes_received;
    }

    if (bytes_received < 0) {
        perror("NM-Fetch: recv failed");
        close(ss_sock_fd);
        return 0;
    }
    
    out_content[total_bytes] = '\0'; 
    
    if (strncmp(out_content, "ERROR_FILE_NOT_FOUND", 20) == 0) {
        printf("NM-Fetch: SS reported file not found.\n");
        close(ss_sock_fd);
        return 0;
    }

    close(ss_sock_fd);
    return 1; 
}

/*
 * Helper: NM acts as client to SS to check for write locks.
 */
int check_if_file_is_locked_on_ss(StorageServer ss, const char* filename) {
// ... (This function is unchanged from Task 24) ...
    int ss_sock_fd;
    struct sockaddr_in ss_addr;
    char command[BUFFER_SIZE];
    char ss_response[BUFFER_SIZE];
    
    sprintf(command, "CHECK_FILE_LOCKED %s", filename);

    ss_sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (ss_sock_fd < 0) {
        perror("NM-CheckLock: socket failed");
        return 0; // Assume unlocked on error
    }

    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_port = htons(ss.port);
    if (inet_pton(AF_INET, ss.ip, &ss_addr.sin_addr) <= 0) {
        perror("NM-CheckLock: invalid address");
        close(ss_sock_fd);
        return 0;
    }

    if (connect(ss_sock_fd, (struct sockaddr *)&ss_addr, sizeof(ss_addr)) < 0) {
        perror("NM-CheckLock: connect failed");
        close(ss_sock_fd);
        return 0;
    }

    if (send(ss_sock_fd, command, strlen(command), 0) < 0) {
        perror("NM-CheckLock: send failed");
        close(ss_sock_fd);
        return 0;
    }

    memset(ss_response, 0, BUFFER_SIZE);
    if (recv(ss_sock_fd, ss_response, BUFFER_SIZE - 1, 0) <= 0) {
        perror("NM-CheckLock: recv failed");
        close(ss_sock_fd);
        return 0;
    }
    
    close(ss_sock_fd);

    if (strncmp(ss_response, "LOCKED", 6) == 0) {
        return 1; // True, it is locked
    }
    
    return 0; // False, it is unlocked
}

void update_access_details(char* filename, char* username) {
    unsigned int idx;
    Node* node = find_file_node(filename, &idx); // This locks the bucket
    if (node) {
        node->file.last_accessed_time = time(NULL);
        strncpy(node->file.last_accessed_by, username, BUFFER_SIZE - 1);
        node->file.last_accessed_by[BUFFER_SIZE - 1] = '\0';
        
        // Update cache with new details
        cache_put(&node->file);
        
        pthread_mutex_unlock(&map_mutexes[idx]);
        save_metadata_to_disk(); 
    } else {
        // find_file_node unlocks if NOT found, so we don't need to unlock here.
    }
}

//advance
int ss_copy_file(StorageServer ss, char* src, char* dest) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return 0;
    
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(ss.port);
    inet_pton(AF_INET, ss.ip, &addr.sin_addr);
    
    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock);
        return 0;
    }
    
    char cmd[BUFFER_SIZE];
    sprintf(cmd, "SS_COPY %s %s", src, dest);
    send(sock, cmd, strlen(cmd), 0);
    
    char buf[64] = {0};
    recv(sock, buf, 63, 0);
    close(sock);
    
    return (strncmp(buf, "ACK_COPY", 8) == 0);
}
/* Helper: Pushes content to a Storage Server (for replication) */
void ss_send_file_content(StorageServer dest, char* filename, char* content, int size) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return;
    
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(dest.port);
    inet_pton(AF_INET, dest.ip, &addr.sin_addr);
    
    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock); return;
    }
    
    char cmd[BUFFER_SIZE];
    sprintf(cmd, "SS_SAVE %s", filename);
    send(sock, cmd, strlen(cmd), 0);
    
    char ack[64];
    recv(sock, ack, 63, 0); // Wait for READY_TO_RECEIVE
    
    if (strncmp(ack, "READY_TO_RECEIVE", 16) == 0) {
        send(sock, content, size, 0);
    }
    close(sock);
}
//advance
/*
 * Thread handler for all clients (SS and UC)
 */
void *handle_client(void *arg) {
    int client_socket = (intptr_t)arg;
    char buffer[BUFFER_SIZE];
    char client_username[BUFFER_SIZE];
    int is_user_client = 0;
    char log_buffer[BUFFER_SIZE + 512];

    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    getpeername(client_socket, (struct sockaddr *)&client_addr, &client_len);

    sprintf(log_buffer, "Handler: Thread started for client %s:%d",
           inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
    log_message(log_buffer);
    printf("Handler: Thread started for client %s:%d\n",
           inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

    // 1. Receive registration message
    memset(buffer, 0, BUFFER_SIZE);
    int bytes_received = recv(client_socket, buffer, BUFFER_SIZE - 1, 0);

    if (bytes_received <= 0) {
        sprintf(log_buffer, "Handler: Client %s:%d disconnected immediately.",
               inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
        log_message(log_buffer);
        printf("Handler: Client disconnected immediately.\n");
        close(client_socket);
        return NULL;
    }

    // --- 2. Parse and Store Client Info ---
    if (strncmp(buffer, "SS ", 3) == 0) {
        // Storage Server registration and file-stream handling
        is_user_client = 0;
        char *token = strtok(buffer, " ");
        char *ip = strtok(NULL, " ");
        char *port_str = strtok(NULL, " ");
        StorageServer new_ss;

        if (ip != NULL && port_str != NULL) {
            pthread_mutex_lock(&list_mutex);
            if (ss_count < MAX_CLIENTS) {
                strncpy(new_ss.ip, ip, INET_ADDRSTRLEN - 1); new_ss.ip[INET_ADDRSTRLEN-1] = '\0';
                new_ss.port = atoi(port_str);
                ss_list[ss_count] = new_ss;
                ss_count++;

                sprintf(log_buffer, "Handler: Registered new SS at %s:%d. Total: %d",
                       new_ss.ip, new_ss.port, ss_count);
                log_message(log_buffer);
                printf("Handler: Registered new SS at %s:%d. Total: %d\n",
                       new_ss.ip, new_ss.port, ss_count);
            } else {
                log_message("Handler: Storage Server list is full.");
                printf("Handler: Storage Server list is full.\n");
                pthread_mutex_unlock(&list_mutex);
                close(client_socket);
                return NULL;
            }
            pthread_mutex_unlock(&list_mutex);
        } else {
            sprintf(log_buffer, "Handler: Malformed SS registration: %s", buffer);
            log_message(log_buffer);
            printf("Handler: Malformed SS registration: %s\n", buffer);
            close(client_socket);
            return NULL;
        }

        if (send(client_socket, "ACK_REG", 7, 0) < 0) {
            perror("Handler: Failed to send ACK_REG to SS");
            log_message("Handler: Failed to send ACK_REG to SS.");
            close(client_socket);
            return NULL;
        }

        // Stream of filenames (with optional metadata) from SS
        int new_files_found = 0;
        char filename_buffer[BUFFER_SIZE];

        while (1) {
            memset(filename_buffer, 0, BUFFER_SIZE);
            bytes_received = recv(client_socket, filename_buffer, BUFFER_SIZE - 1, 0);

            if (bytes_received <= 0) {
                log_message("Handler: SS disconnected during file list stream.");
                printf("Handler: SS disconnected during file list stream.\n");
                break;
            }

            // Check for completion token
            if (strncmp(filename_buffer, SS_FILE_LIST_COMPLETE, strlen(SS_FILE_LIST_COMPLETE)) == 0) {
                log_message("Handler: Received SS_FILE_LIST_COMPLETE.");
                break;
            }

            // sanitize incoming buffer (remove trailing newline/CR)
            filename_buffer[strcspn(filename_buffer, "\r\n")] = '\0';

            // incoming token formats supported:
            // name
            // name|size
            // name|size|mtime
            // name|size|mtime|words|chars
            char parsed_name[BUFFER_SIZE] = {0};
            long parsed_size = 0;
            time_t parsed_mtime = time(NULL);
            long parsed_words = 0;
            long parsed_chars = 0;

            char tmp_buf[BUFFER_SIZE];
            strncpy(tmp_buf, filename_buffer, BUFFER_SIZE - 1);
            tmp_buf[BUFFER_SIZE - 1] = '\0';

            char *saveptr = NULL;
            char *tok = strtok_r(tmp_buf, "|", &saveptr);
            if (tok != NULL) {
                // name
                strncpy(parsed_name, tok, BUFFER_SIZE - 1);
                parsed_name[BUFFER_SIZE - 1] = '\0';

                // size
                tok = strtok_r(NULL, "|", &saveptr);
                if (tok != NULL) parsed_size = strtol(tok, NULL, 10);

                // mtime
                tok = strtok_r(NULL, "|", &saveptr);
                if (tok != NULL) parsed_mtime = (time_t)strtoul(tok, NULL, 10);

                // words
                tok = strtok_r(NULL, "|", &saveptr);
                if (tok != NULL) parsed_words = strtol(tok, NULL, 10);

                // chars
                tok = strtok_r(NULL, "|", &saveptr);
                if (tok != NULL) parsed_chars = strtol(tok, NULL, 10);
            }

            unsigned int index;
            Node* file_node = find_file_node(parsed_name, &index);

            if (file_node == NULL) {
                new_files_found++;

                FileInfo new_file;
                memset(&new_file, 0, sizeof(new_file));
                strncpy(new_file.filename, parsed_name, BUFFER_SIZE - 1);
                new_file.filename[BUFFER_SIZE - 1] = '\0';
                new_file.location = new_ss;
                strncpy(new_file.owner_username, "storage_server", BUFFER_SIZE - 1);
                new_file.owner_username[BUFFER_SIZE - 1] = '\0';
                new_file.file_size = parsed_size;
                new_file.creation_time = parsed_mtime;
                new_file.last_modified_time = parsed_mtime;
                new_file.word_count = parsed_words;
                new_file.char_count = parsed_chars;
                new_file.access_count = 1;
                strncpy(new_file.access_list[0].username, "storage_server", BUFFER_SIZE - 1);
                new_file.access_list[0].username[BUFFER_SIZE - 1] = '\0';
                new_file.access_list[0].access_type = 'W';

                Node* new_node = (Node*)malloc(sizeof(Node));
                if (new_node) {
                    new_node->file = new_file;
                    new_node->file.is_directory = 0;
                    strcpy(new_node->file.parent_dir, "root");
                    // --- ADD THIS ---
                    new_node->file.request_count = 0;
                    // ----------------
                    pthread_mutex_lock(&map_mutexes[index]);
                    new_node->next = file_hash_map[index];
                    file_hash_map[index] = new_node;
                    pthread_mutex_unlock(&map_mutexes[index]);
                }
            } else {
                // found existing file; unlock the bucket (find_file_node may have left it locked)
                pthread_mutex_unlock(&map_mutexes[index]);
            }

            // ACK this file entry
            if (send(client_socket, "ACK_FILE", 8, 0) < 0) {
                log_message("Handler: Failed to send ACK_FILE to SS. Aborting stream.");
                break;
            }
        } // end streaming loop

        if (new_files_found > 0) {
            sprintf(log_buffer, "Handler: Registered %d new files from SS.", new_files_found);
            log_message(log_buffer);
            save_metadata_to_disk();
        }

        close(client_socket);
        sprintf(log_buffer, "Handler: SS %s:%d finished registration, thread closing.",
               inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
        log_message(log_buffer);
        printf("Handler: SS %s:%d finished registration, thread closing.\n",
               inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
        return NULL;

    } else if (strncmp(buffer, "UPDATE_META ", 12) == 0) {
        // Format: UPDATE_META <filename> <size> <words> <chars>
        char temp_buf[BUFFER_SIZE];
        // Note: Use 'buffer' here, not 'command_buffer'
        strncpy(temp_buf, buffer, BUFFER_SIZE-1); 
        temp_buf[BUFFER_SIZE-1] = '\0';

        char *cmd = strtok(temp_buf, " ");
        char *fname = strtok(NULL, " ");
        char *size_str = strtok(NULL, " ");
        char *words_str = strtok(NULL, " ");
        char *chars_str = strtok(NULL, " ");

        if (fname && size_str && words_str && chars_str) {
            unsigned int idx;
            Node* node = find_file_node(fname, &idx);
            
            if (node) {
                node->file.file_size = atol(size_str);
                node->file.word_count = atol(words_str);
                node->file.char_count = atol(chars_str);
                node->file.last_modified_time = time(NULL); 

                //advance
                // --- PART 4: ASYNC REPLICATION SYNC ---
                if (node->file.has_backup) {
                     // 1. Fetch latest content from Main SS
                     char *temp_content = malloc(MAX_FILE_SIZE);
                     if (temp_content) {
                         if (fetch_file_content_from_ss(node->file.location, fname, temp_content, MAX_FILE_SIZE)) {
                             // 2. Push content to Backup SS
                             ss_send_file_content(node->file.backup_location, fname, temp_content, strlen(temp_content));
                             log_message("Handler: Syncronized replication to backup.");
                         }
                         free(temp_content);
                     }
                }
                // --------------------------------------
                
                cache_put(&node->file);
                
                pthread_mutex_unlock(&map_mutexes[idx]);
                
                sprintf(log_buffer, "Handler: Updated metadata for %s (W:%ld C:%ld)", 
                        fname, node->file.word_count, node->file.char_count);
                log_message(log_buffer);
                save_metadata_to_disk(); 
            } else {
                // Node not found (mutex already handled by find_file_node logic in your code)
            }
        }
        close(client_socket);
        return NULL;    

    } else {
        // User client registration
        is_user_client = 1;
        strncpy(client_username, buffer, BUFFER_SIZE - 1);
        client_username[BUFFER_SIZE - 1] = '\0';

        pthread_mutex_lock(&list_mutex);

        if (online_count < MAX_CLIENTS) {
            strncpy(online_users[online_count].username, client_username, BUFFER_SIZE - 1);
            online_users[online_count].username[BUFFER_SIZE - 1] = '\0';
            online_count++;
            sprintf(log_buffer, "Handler: User '%s' connected. Total online: %d", client_username, online_count);
            log_message(log_buffer);
            printf("Handler: Registered new User: %s. Total online: %d\n", client_username, online_count);
        } else {
            log_message("Handler: Online user list is full.");
            printf("Handler: Online user list is full.\n");
            is_user_client = 0;
        }

        int found_in_all_users = 0;
        for (int i = 0; i < all_users_count; i++) {
            if (strcmp(all_users[i].username, client_username) == 0) {
                found_in_all_users = 1;
                break;
            }
        }

        if (!found_in_all_users) {
            if (all_users_count < MAX_CLIENTS) {
                strncpy(all_users[all_users_count].username, client_username, BUFFER_SIZE - 1);
                all_users[all_users_count].username[BUFFER_SIZE - 1] = '\0';
                all_users_count++;
                sprintf(log_buffer, "Handler: New user '%s' added to persistent list. Total all users: %d", client_username, all_users_count);
                log_message(log_buffer);
                save_users_to_disk();
            } else {
                log_message("Handler: Persistent user list is full.");
            }
        }
        pthread_mutex_unlock(&list_mutex);
    }

    // --- 3. Command Loop for User Clients ---
    if (is_user_client) {
        char command_buffer[BUFFER_SIZE];
        char response_buffer[RESPONSE_SIZE];
        char line_buffer[512];
        char time_str[100];
        struct tm *tm_info;

        while (1) {
            memset(command_buffer, 0, BUFFER_SIZE);
            bytes_received = recv(client_socket, command_buffer, BUFFER_SIZE - 1, 0);

            if (bytes_received <= 0) {
                sprintf(log_buffer, "Handler: User '%s' disconnected.", client_username);
                log_message(log_buffer);
                printf("Handler: User '%s' disconnected.\n", client_username);
                break;
            }

            command_buffer[strcspn(command_buffer, "\n")] = '\0';

            snprintf(log_buffer, sizeof(log_buffer), "Handler: Received command from '%.512s': %.512s",
                     client_username, command_buffer);
            log_message(log_buffer);
            // ... (Log message code remains above this) ...

            // 1. Check for LISTCHECKPOINTS FIRST (Longer prefix)
            if (strncmp(command_buffer, "LISTCHECKPOINTS ", 16) == 0) {
                printf("Handler: Received LISTCHECKPOINTS from '%s'\n", client_username);
                char temp_cmd[BUFFER_SIZE]; strcpy(temp_cmd, command_buffer);
                strtok(temp_cmd, " "); 
                char *filename = strtok(NULL, " ");

                if (!filename) {
                    send(client_socket, "Usage: LISTCHECKPOINTS <filename>", 33, 0);
                    continue;
                }

                // Verify file exists
                unsigned int idx;
                Node* node = find_file_node(filename, &idx);
                if (node) pthread_mutex_unlock(&map_mutexes[idx]);
                else {
                     send(client_socket, "File not found.", 15, 0);
                     continue;
                }

                // Search for checkpoints
                memset(response_buffer, 0, RESPONSE_SIZE);
                snprintf(response_buffer, RESPONSE_SIZE, "--- Checkpoints for '%s' ---\n", filename);
                
                char prefix[BUFFER_SIZE];
                sprintf(prefix, "%s_checkpoint_", filename);
                int prefix_len = strlen(prefix);
                int found = 0;

                for (int i = 0; i < HASH_MAP_SIZE; i++) {
                    pthread_mutex_lock(&map_mutexes[i]);
                    Node* curr = file_hash_map[i];
                    while(curr) {
                        if (strncmp(curr->file.filename, prefix, prefix_len) == 0) {
                            char* tag = curr->file.filename + prefix_len;
                            char line[BUFFER_SIZE];
                            sprintf(line, "-> Tag: %s\n", tag);
                            
                            if (strlen(response_buffer) + strlen(line) < RESPONSE_SIZE) {
                                strcat(response_buffer, line);
                                found = 1;
                            }
                        }
                        curr = curr->next;
                    }
                    pthread_mutex_unlock(&map_mutexes[i]);
                }

                if (!found) strcat(response_buffer, "(No checkpoints found)\n");
                send(client_socket, response_buffer, strlen(response_buffer), 0);

            // 2. Check for standard LIST SECOND (Shorter prefix)
            } else if (strncmp(command_buffer, "LIST", 4) == 0) {
                printf("Handler: Received LIST from '%s'\n", client_username);
                memset(response_buffer, 0, RESPONSE_SIZE);
                strcat(response_buffer, "--- All Registered Users ---\n");

                pthread_mutex_lock(&list_mutex);
                for (int i = 0; i < all_users_count; i++) {
                    if (strlen(response_buffer) + strlen(all_users[i].username) + 2 < RESPONSE_SIZE) {
                        strcat(response_buffer, all_users[i].username);
                        strcat(response_buffer, "\n");
                    }
                }
                pthread_mutex_unlock(&list_mutex);

                send(client_socket, response_buffer, strlen(response_buffer), 0);
                sprintf(log_buffer, "Handler: Sent All Users LIST response to '%s'", client_username);
                log_message(log_buffer);

            } else if (strncmp(command_buffer, "EXIT", 4) == 0) {
                // ... (Rest of your code continues normally)
                printf("Handler: User '%s' sent EXIT.\n", client_username);
                break;

            } else if (strncmp(command_buffer, "CREATE ", 7) == 0) {
                printf("Handler: Received CREATE from '%s'\n", client_username);

                char temp_cmd[BUFFER_SIZE];
                strcpy(temp_cmd, command_buffer);
                char *cmd = strtok(temp_cmd, " ");
                char *filename = strtok(NULL, " ");

                if (filename == NULL) {
                    const char* err_msg = "ERROR: CREATE requires a filename. Usage: CREATE <filename>";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    log_message("Handler: Sent ERROR (CREATE usage) to client.");
                    continue;
                }

                unsigned int index;
                Node* file_node = find_file_node(filename, &index);

                if (file_node != NULL) {
                    pthread_mutex_unlock(&map_mutexes[index]);
                    const char* err_msg = "ERROR: File already exists.";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    log_message("Handler: Sent ERROR (File already exists) to client.");
                    continue;
                }

                pthread_mutex_lock(&list_mutex);
                if (ss_count == 0) {
                    pthread_mutex_unlock(&list_mutex);
                    const char *err_msg = "ERROR: No Storage Servers are available.";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    log_message("Handler: Sent ERROR (No SS available) to client.");
                    continue;
                }
                StorageServer target_ss = ss_list[0];
                //advance
                StorageServer backup_ss;
                int has_back = 0;
                
                // Simple Strategy: If we have >1 SS, use the second one as backup
                if (ss_count > 1) {
                    backup_ss = ss_list[1];
                    has_back = 1;
                }
                //advance
                pthread_mutex_unlock(&list_mutex);

                sprintf(log_buffer, "Handler: Proxying CREATE to SS at %s:%d", target_ss.ip, target_ss.port);
                log_message(log_buffer);
                printf("Handler: Proxying CREATE to SS at %s:%d\n", target_ss.ip, target_ss.port);

                int success = proxy_command_to_ss(target_ss.ip, target_ss.port, command_buffer);

                if (success) {
                    FileInfo new_file;
                    memset(&new_file,0,sizeof(new_file));
                    strncpy(new_file.filename, filename, BUFFER_SIZE - 1);
                    new_file.location = target_ss;
                    strncpy(new_file.owner_username, client_username, BUFFER_SIZE - 1);
                    new_file.file_size = 0;
                    new_file.creation_time = time(NULL);
                    new_file.last_modified_time = time(NULL);
                    new_file.last_accessed_time = time(NULL);
                    strncpy(new_file.last_accessed_by, client_username, BUFFER_SIZE - 1);
                    new_file.last_accessed_by[BUFFER_SIZE - 1] = '\0';
                    new_file.word_count = 0;
                    new_file.char_count = 0;
                    new_file.access_count = 1;
                    strncpy(new_file.access_list[0].username, client_username, BUFFER_SIZE - 1);
                    new_file.access_list[0].access_type = 'W';

                    new_file.is_directory = 0;
                    strcpy(new_file.parent_dir, "root");
                    // --- ADD THIS ---
                    new_file.request_count = 0;
                    new_file.has_backup = has_back;
                    if (has_back) new_file.backup_location = backup_ss;
                    if (has_back) {
                        proxy_command_to_ss(backup_ss.ip, backup_ss.port, command_buffer);
                        sprintf(log_buffer, "Handler: Replicated file '%s' on Backup SS %d", filename, backup_ss.port);
                        log_message(log_buffer);
                    }
                    // ----------------
                    Node* new_node = (Node*)malloc(sizeof(Node));
                    if (new_node == NULL) {
                        log_message("Handler: Malloc failed for new file node.");
                        send(client_socket, "ERROR: Internal server error.", 28, 0);
                        continue;
                    }
                    new_node->file = new_file;

                    index = hash(filename);
                    pthread_mutex_lock(&map_mutexes[index]);

                    new_node->next = file_hash_map[index];
                    file_hash_map[index] = new_node;

                    pthread_mutex_unlock(&map_mutexes[index]);

                    cache_put(&new_node->file);
                    save_metadata_to_disk();

                    sprintf(log_buffer, "Handler: File '%s' added to metadata map.", filename);
                    log_message(log_buffer);
                    printf("Handler: File '%s' added to metadata map.\n", filename);

                    const char *ok_msg = "File created successfully!";
                    send(client_socket, ok_msg, strlen(ok_msg), 0);
                } else {
                    const char *err_msg = "ERROR: Storage Server failed to create file.";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    log_message("Handler: Sent ERROR (SS create failed) to client.");
                }

            } else if (strncmp(command_buffer, "VIEWFOLDER ", 11) == 0) {
                printf("Handler: Received VIEWFOLDER from '%s'\n", client_username);
                char temp_cmd[BUFFER_SIZE];
                strcpy(temp_cmd, command_buffer);
                char *cmd = strtok(temp_cmd, " ");
                char *foldername = strtok(NULL, " ");

                if (foldername == NULL) {
                    send(client_socket, "ERROR: Usage: VIEWFOLDER <foldername>", 37, 0);
                    continue;
                }

                // Verify folder exists (optional, but good for UX)
                unsigned int f_idx;
                Node* f_node = find_file_node(foldername, &f_idx);
                if (f_node == NULL && strcmp(foldername, "root") != 0) {
                     send(client_socket, "ERROR: Folder not found.", 24, 0);
                     continue;
                }
                if (f_node) pthread_mutex_unlock(&map_mutexes[f_idx]);

                // Search ALL buckets for files with parent_dir == foldername
                memset(response_buffer, 0, RESPONSE_SIZE);
                snprintf(response_buffer, RESPONSE_SIZE, "--- Files in '%s' ---\n", foldername);
                
                int found_any = 0;
                for (int i = 0; i < HASH_MAP_SIZE; i++) {
                    pthread_mutex_lock(&map_mutexes[i]);
                    Node* curr = file_hash_map[i];
                    while (curr != NULL) {
                        if (strcmp(curr->file.parent_dir, foldername) == 0) {
                            // Append logic (check for overflow)
                            if (strlen(response_buffer) + strlen(curr->file.filename) + 5 < RESPONSE_SIZE) {
                                strcat(response_buffer, curr->file.filename);
                                if (curr->file.is_directory) strcat(response_buffer, "/"); // Mark subfolders
                                strcat(response_buffer, "\n");
                                found_any = 1;
                            }
                        }
                        curr = curr->next;
                    }
                    pthread_mutex_unlock(&map_mutexes[i]);
                }

                if (!found_any) strcat(response_buffer, "(Empty folder)\n");
                send(client_socket, response_buffer, strlen(response_buffer), 0);
                // --- PART 2: CHECKPOINTS ---
            } else if (strncmp(command_buffer, "CHECKPOINT ", 11) == 0) {
                char temp_cmd[BUFFER_SIZE]; strcpy(temp_cmd, command_buffer);
                strtok(temp_cmd, " ");
                char *filename = strtok(NULL, " ");
                char *tag = strtok(NULL, " ");

                if (!filename || !tag) {
                    send(client_socket, "Usage: CHECKPOINT <file> <tag>", 30, 0);
                    continue;
                }
                
                unsigned int idx;
                Node* node = find_file_node(filename, &idx);
                if (!node) {
                    send(client_socket, "File not found.", 15, 0);
                    continue;
                }
                
                // Capture necessary data and UNLOCK immediately to prevent deadlock
                StorageServer source_ss = node->file.location;
                FileInfo original_metadata = node->file;
                pthread_mutex_unlock(&map_mutexes[idx]); // <--- CRITICAL UNLOCK
                
                char cp_name[BUFFER_SIZE];
                sprintf(cp_name, "%s_checkpoint_%s", filename, tag);
                
                // Now checks if this checkpoint tag already exists
                unsigned int cp_idx;
                Node* exist = find_file_node(cp_name, &cp_idx);
                if (exist) {
                    pthread_mutex_unlock(&map_mutexes[cp_idx]);
                    // No need to unlock idx, it's already unlocked
                    send(client_socket, "Checkpoint tag already exists.", 30, 0);
                    continue;
                }
                
                // Send Copy Command to SS
                if (ss_copy_file(source_ss, filename, cp_name)) {
                    // Create Metadata Entry for Checkpoint
                    Node* cp_node = (Node*)malloc(sizeof(Node));
                    
                    // Copy metadata from the saved struct
                    cp_node->file = original_metadata; 
                    strcpy(cp_node->file.filename, cp_name);
                    strcpy(cp_node->file.parent_dir, "checkpoints"); 
                    cp_node->file.is_directory = 0;
                    
                    // Link new node
                    // Note: find_file_node leaves cp_idx locked if not found? 
                    // No, typically find_file_node unlocks if NULL. 
                    // So we must LOCK cp_idx manually here.
                    pthread_mutex_lock(&map_mutexes[cp_idx]);
                    
                    cp_node->next = file_hash_map[cp_idx];
                    file_hash_map[cp_idx] = cp_node;
                    
                    pthread_mutex_unlock(&map_mutexes[cp_idx]);
                    
                    save_metadata_to_disk();
                    send(client_socket, "Checkpoint created successfully.", 32, 0);
                } else {
                    send(client_socket, "Storage Server failed to create checkpoint.", 43, 0);
                }
             //advance
             // --- PART 3: REQUESTING ACCESS ---
 // --- PART 3: REQUESTING ACCESS (Deadlock Fixed & VIEWREQ Added) ---
            } else if (strncmp(command_buffer, "REQACCESS ", 10) == 0) {
                char temp_cmd[BUFFER_SIZE]; strcpy(temp_cmd, command_buffer);
                strtok(temp_cmd, " ");
                char *filename = strtok(NULL, " ");
                char *perm = strtok(NULL, " "); // "-R" or "-W"

                if (!filename || !perm) { send(client_socket, "Usage: REQACCESS <file> <-R|-W>", 31, 0); continue; }
                
                char type = perm[1]; // 'R' or 'W'
                if (type != 'R' && type != 'W') { send(client_socket, "Invalid permission type.", 24, 0); continue; }

                unsigned int idx;
                Node* node = find_file_node(filename, &idx);
                if (!node) { send(client_socket, "File not found.", 15, 0); continue; }
                
                if (check_permission(client_username, &node->file, type)) {
                    pthread_mutex_unlock(&map_mutexes[idx]);
                    send(client_socket, "You already have access.", 24, 0);
                    continue;
                }

                if (node->file.request_count < MAX_CLIENTS) {
                    int already = 0;
                    for(int i=0; i<node->file.request_count; i++) {
                        if(strcmp(node->file.pending_requests[i].username, client_username) == 0) {
                            already = 1; break;
                        }
                    }
                    if (!already) {
                        strcpy(node->file.pending_requests[node->file.request_count].username, client_username);
                        node->file.pending_requests[node->file.request_count].request_type = type;
                        node->file.request_count++;
                        
                        // UNLOCK BEFORE SAVE TO FIX DEADLOCK
                        pthread_mutex_unlock(&map_mutexes[idx]);
                        save_metadata_to_disk();
                        send(client_socket, "Request sent successfully.", 26, 0);
                    } else {
                         pthread_mutex_unlock(&map_mutexes[idx]);
                         send(client_socket, "Request already pending.", 24, 0);
                    }
                } else {
                    pthread_mutex_unlock(&map_mutexes[idx]);
                    send(client_socket, "Request list full.", 18, 0);
                }

            } else if (strncmp(command_buffer, "VIEWREQ", 7) == 0) {
                // List requests for files OWNED by this client
                memset(response_buffer, 0, RESPONSE_SIZE);
                strcat(response_buffer, "--- Pending Requests ---\n");
                
                int found = 0;
                for (int i = 0; i < HASH_MAP_SIZE; i++) {
                    pthread_mutex_lock(&map_mutexes[i]);
                    Node* curr = file_hash_map[i];
                    while(curr) {
                        // Only show if I am the owner AND there are requests
                        if (strcmp(curr->file.owner_username, client_username) == 0 && curr->file.request_count > 0) {
                            for(int j=0; j<curr->file.request_count; j++) {
                                char line[256];
                                sprintf(line, "File: %s | User: %s | Type: %c\n", 
                                        curr->file.filename, 
                                        curr->file.pending_requests[j].username,
                                        curr->file.pending_requests[j].request_type);
                                // check buffer space
                                if (strlen(response_buffer) + strlen(line) < RESPONSE_SIZE) {
                                    strcat(response_buffer, line);
                                    found = 1;
                                }
                            }
                        }
                        curr = curr->next;
                    }
                    pthread_mutex_unlock(&map_mutexes[i]);
                }
                if(!found) strcat(response_buffer, "(No pending requests)\n");
                send(client_socket, response_buffer, strlen(response_buffer), 0);

            } else if (strncmp(command_buffer, "APPROVEREQ ", 11) == 0) {
                char temp_cmd[BUFFER_SIZE]; strcpy(temp_cmd, command_buffer);
                strtok(temp_cmd, " ");
                char *filename = strtok(NULL, " ");
                char *target_user = strtok(NULL, " ");

                if (!filename || !target_user) { send(client_socket, "Usage: APPROVEREQ <file> <user>", 31, 0); continue; }

                unsigned int idx;
                Node* node = find_file_node(filename, &idx);
                if (!node) { send(client_socket, "File not found.", 15, 0); continue; }
                
                if (strcmp(node->file.owner_username, client_username) != 0) {
                    pthread_mutex_unlock(&map_mutexes[idx]);
                    send(client_socket, "Only owner can approve.", 23, 0);
                    continue;
                }

                int req_idx = -1;
                for(int i=0; i<node->file.request_count; i++) {
                    if(strcmp(node->file.pending_requests[i].username, target_user) == 0) {
                        req_idx = i; break;
                    }
                }

                if (req_idx != -1) {
                    char type = node->file.pending_requests[req_idx].request_type;
                    
                    // Add to access list
                    int acc_idx = -1;
                    for(int k=0; k<node->file.access_count; k++) {
                        if(strcmp(node->file.access_list[k].username, target_user) == 0) {
                            acc_idx = k; break;
                        }
                    }
                    if(acc_idx != -1) {
                        node->file.access_list[acc_idx].access_type = type;
                    } else if (node->file.access_count < MAX_CLIENTS) {
                         strcpy(node->file.access_list[node->file.access_count].username, target_user);
                         node->file.access_list[node->file.access_count].access_type = type;
                         node->file.access_count++;
                    }
                    
                    // Remove from request list
                    for(int i=req_idx; i<node->file.request_count-1; i++) {
                        node->file.pending_requests[i] = node->file.pending_requests[i+1];
                    }
                    node->file.request_count--;
                    
                    cache_invalidate(filename);
                    // UNLOCK BEFORE SAVE
                    pthread_mutex_unlock(&map_mutexes[idx]);
                    save_metadata_to_disk();
                    send(client_socket, "Request approved. Access granted.", 33, 0);
                } else {
                    pthread_mutex_unlock(&map_mutexes[idx]);
                    send(client_socket, "Request not found.", 18, 0);
                }

            } else if (strncmp(command_buffer, "REJECTREQ ", 10) == 0) {
                char temp_cmd[BUFFER_SIZE]; strcpy(temp_cmd, command_buffer);
                strtok(temp_cmd, " ");
                char *filename = strtok(NULL, " ");
                char *target_user = strtok(NULL, " ");

                if (!filename || !target_user) { send(client_socket, "Usage: REJECTREQ <file> <user>", 30, 0); continue; }

                unsigned int idx;
                Node* node = find_file_node(filename, &idx);
                if (!node) { send(client_socket, "File not found.", 15, 0); continue; }

                if (strcmp(node->file.owner_username, client_username) != 0) {
                    pthread_mutex_unlock(&map_mutexes[idx]);
                    send(client_socket, "Only owner can reject.", 22, 0);
                    continue;
                }

                int req_idx = -1;
                for(int i=0; i<node->file.request_count; i++) {
                    if(strcmp(node->file.pending_requests[i].username, target_user) == 0) {
                        req_idx = i; break;
                    }
                }

                if (req_idx != -1) {
                    for(int i=req_idx; i<node->file.request_count-1; i++) {
                        node->file.pending_requests[i] = node->file.pending_requests[i+1];
                    }
                    node->file.request_count--;
                    
                    // UNLOCK BEFORE SAVE
                    pthread_mutex_unlock(&map_mutexes[idx]);
                    save_metadata_to_disk();
                    send(client_socket, "Request rejected.", 17, 0);
                } else {
                    pthread_mutex_unlock(&map_mutexes[idx]);
                    send(client_socket, "Request not found.", 18, 0);
                }
                //advance
            } else if (strncmp(command_buffer, "REVERT ", 7) == 0) {
                char temp_cmd[BUFFER_SIZE]; strcpy(temp_cmd, command_buffer);
                strtok(temp_cmd, " ");
                char *filename = strtok(NULL, " ");
                char *tag = strtok(NULL, " ");
                
                if (!filename || !tag) { send(client_socket, "Usage: REVERT <file> <tag>", 26, 0); continue; }

                char cp_name[BUFFER_SIZE];
                sprintf(cp_name, "%s_checkpoint_%s", filename, tag);
                
                unsigned int idx;
                Node* node = find_file_node(filename, &idx);
                if (!node) { send(client_socket, "File not found.", 15, 0); continue; }
                StorageServer ss = node->file.location;
                pthread_mutex_unlock(&map_mutexes[idx]); // Unlock original

                unsigned int cp_idx;
                Node* cp_node = find_file_node(cp_name, &cp_idx);
                if (!cp_node) { send(client_socket, "Checkpoint not found.", 21, 0); continue; }
                
                // Capture metadata from the checkpoint (size, words, etc.)
                FileInfo cp_info = cp_node->file;
                pthread_mutex_unlock(&map_mutexes[cp_idx]); 

                if (ss_copy_file(ss, cp_name, filename)) {
                    // --- METADATA UPDATE FIX ---
                    // Re-lock original file to update its stats to match the checkpoint
                    pthread_mutex_lock(&map_mutexes[idx]);
                    Node* orig_node = file_hash_map[idx];
                    while(orig_node) {
                        if(strcmp(orig_node->file.filename, filename) == 0) {
                            orig_node->file.file_size = cp_info.file_size;
                            orig_node->file.word_count = cp_info.word_count;
                            orig_node->file.char_count = cp_info.char_count;
                            orig_node->file.last_modified_time = time(NULL);
                            break;
                        }
                        orig_node = orig_node->next;
                    }
                    pthread_mutex_unlock(&map_mutexes[idx]);
                    save_metadata_to_disk();
                    // ---------------------------

                    cache_invalidate(filename);
                    send(client_socket, "Reverted successfully.", 22, 0);
                } else {
                    send(client_socket, "Revert failed.", 14, 0);
                }

            } else if (strncmp(command_buffer, "VIEWCHECKPOINT ", 15) == 0) {
                char temp_cmd[BUFFER_SIZE]; strcpy(temp_cmd, command_buffer);
                char *cmd = strtok(temp_cmd, " ");
                char *filename = strtok(NULL, " ");
                char *tag = strtok(NULL, " ");
                
                if (!filename || !tag) { send(client_socket, "Usage: VIEWCHECKPOINT <file> <tag>", 34, 0); continue; }

                char cp_name[BUFFER_SIZE];
                sprintf(cp_name, "%s_checkpoint_%s", filename, tag);
                
                unsigned int idx;
                Node* node = find_file_node(cp_name, &idx);
                if(node) {
                    StorageServer ss = node->file.location;
                    pthread_mutex_unlock(&map_mutexes[idx]);
                    
                    // Fetch content directly and send to client
                    char content[MAX_FILE_SIZE];
                    if (fetch_file_content_from_ss(ss, cp_name, content, MAX_FILE_SIZE)) {
                         send(client_socket, content, strlen(content), 0);
                    } else {
                         send(client_socket, "Error fetching content.", 23, 0);
                    }
                } else {
                    send(client_socket, "Checkpoint not found.", 21, 0);
                }
            
                
            }
            else if (strncmp(command_buffer, "VIEW", 4) == 0) {
                printf("Handler: Received VIEW from '%s'\n", client_username);
                /* parse flags robustly so "VIEW -al", "VIEW -la", "VIEW -a -l" all work */
                int flag_a = 0, flag_l = 0;
                char cmdcpy[BUFFER_SIZE];
                strncpy(cmdcpy, command_buffer, BUFFER_SIZE - 1); cmdcpy[BUFFER_SIZE-1] = '\0';
                char *tk = strtok(cmdcpy, " "); // "VIEW"
                while ((tk = strtok(NULL, " ")) != NULL) {
                    if (tk[0] == '-') {
                        for (int i = 1; tk[i] != '\0'; i++) {
                            if (tk[i] == 'a') flag_a = 1;
                            if (tk[i] == 'l') flag_l = 1;
                        }
                    }
                }

                char response_buffer[RESPONSE_SIZE];
                char line_buffer[256];
                char time_str[64];
                struct tm *tm_info;
                memset(response_buffer, 0, sizeof(response_buffer));

                if (flag_l) {
                    strcat(response_buffer, "|  Filename  | Words | Chars | Last Access Time | Owner       |\n");
                    strcat(response_buffer, "|------------|-------|-------|------------------|-------------|\n");
                }

                for (int i = 0; i < HASH_MAP_SIZE; i++) {
                    pthread_mutex_lock(&map_mutexes[i]);
                    Node* cur = file_hash_map[i];
                    while (cur) {
                        FileInfo *f = &cur->file;
                        if (f == NULL) { cur = cur->next; continue; }

                        /* include file if -a (all files) OR requester has read access or is owner */
                        int include = flag_a || check_permission(client_username, f, 'R');
                        if (!include) { cur = cur->next; continue; }

                        if (flag_l) {
                            /* safe local copies */
                            char fname[64] = ""; strncpy(fname, f->filename, sizeof(fname)-1);
                            char owner[64] = ""; strncpy(owner, f->owner_username, sizeof(owner)-1);

                            if (f->last_modified_time <= 0) {
                                strncpy(time_str, "N/A", sizeof(time_str)); time_str[sizeof(time_str)-1] = '\0';
                            } else {
                                tm_info = localtime(&f->last_modified_time);
                                if (tm_info) strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M", tm_info);
                                else { strncpy(time_str, "N/A", sizeof(time_str)); time_str[sizeof(time_str)-1] = '\0'; }
                            }

                            long words = f->word_count;
                            long chars = f->char_count;
                            if (chars <= 0 && f->file_size > 0) chars = f->file_size; /* fallback */

                            snprintf(line_buffer, sizeof(line_buffer),
                                     "| %-10.10s | %5ld | %5ld | %16s | %-11.11s |\n",
                                     fname, words, chars, time_str, owner);
                        } else {
                            snprintf(line_buffer, sizeof(line_buffer), "%s\n", f->filename);
                        }

                        if (strlen(response_buffer) + strlen(line_buffer) < RESPONSE_SIZE - 1) {
                            strcat(response_buffer, line_buffer);
                        } else {
                            send(client_socket, response_buffer, strlen(response_buffer), 0);
                            memset(response_buffer, 0, sizeof(response_buffer));
                            strcat(response_buffer, line_buffer);
                        }

                        cur = cur->next;
                    }
                    pthread_mutex_unlock(&map_mutexes[i]);
                }

                if (strlen(response_buffer) == 0) strcat(response_buffer, "\n");
                send(client_socket, response_buffer, strlen(response_buffer), 0);
                log_message("Handler: Sent VIEW response to client");

            } else if (strncmp(command_buffer, "INFO ", 5) == 0) {
                printf("Handler: Received INFO from '%s'\n", client_username);
                char temp_cmd[BUFFER_SIZE];
                strcpy(temp_cmd, command_buffer);
                char *filename = strtok(temp_cmd, " ");
                filename = strtok(NULL, " ");
                if (filename == NULL) {
                    const char *err_msg = "ERROR: INFO requires a filename. Usage: INFO <filename>";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    log_message("Handler: Sent ERROR (INFO usage) to client.");
                    continue;
                }

                FileInfo file_copy;
                int found_in_cache = cache_get(filename, &file_copy);

                if (!found_in_cache) {
                    log_message("Handler: Cache Miss (INFO)");
                    unsigned int index;
                    Node* file_node = find_file_node(filename, &index);
                    if (file_node) {
                        file_copy = file_node->file;
                        cache_put(&file_copy);
                        pthread_mutex_unlock(&map_mutexes[index]);
                        found_in_cache = 1;
                    }
                } else {
                    log_message("Handler: Cache Hit (INFO)");
                }

                memset(response_buffer, 0, RESPONSE_SIZE);
                if (!found_in_cache) {
                    sprintf(response_buffer, "ERROR: File not found.\n");
                } else {
                    FileInfo* file = &file_copy;
                    tm_info = localtime(&file->creation_time);
                    if (tm_info) strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", tm_info);
                    else strncpy(time_str, "N/A", sizeof(time_str));
                    sprintf(line_buffer, "--> Created: %s\n", time_str);
                    strcat(response_buffer, line_buffer);

                    tm_info = localtime(&file->last_modified_time);
                    if (tm_info) strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", tm_info);
                    else strncpy(time_str, "N/A", sizeof(time_str));
                    sprintf(line_buffer, "--> Last Modified: %s\n", time_str);
                    strcat(response_buffer, line_buffer);

                    sprintf(line_buffer, "--> File: %.256s\n", file->filename);
                    strcat(response_buffer, line_buffer);
                    sprintf(line_buffer, "--> Owner: %.256s\n", file->owner_username);
                    strcat(response_buffer, line_buffer);
                    sprintf(line_buffer, "--> Size: %ld bytes\n", file->file_size);
                    strcat(response_buffer, line_buffer);

                    for (int i = 0; i < file->access_count; i++) {
                        sprintf(line_buffer, "--> Access: %.256s (%c)\n",
                                file->access_list[i].username,
                                file->access_list[i].access_type);
                        strcat(response_buffer, line_buffer);
                    }
                    tm_info = localtime(&file->last_accessed_time);
                    if (tm_info) strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", tm_info);
                    else strncpy(time_str, "N/A", sizeof(time_str));
                    
                    sprintf(line_buffer, "--> Last Accessed: %s by %s\n", 
                            time_str, file->last_accessed_by);
                    strcat(response_buffer, line_buffer);
                }

                send(client_socket, response_buffer, strlen(response_buffer), 0);
                sprintf(log_buffer, "Handler: Sent INFO response for %s to client.", filename);
                log_message(log_buffer);

            } else if (strncmp(command_buffer, "DELETE ", 7) == 0) {
                printf("Handler: Received DELETE from '%s'\n", client_username);
                char temp_cmd[BUFFER_SIZE];
                strcpy(temp_cmd, command_buffer);
                char *filename = strtok(temp_cmd, " ");
                filename = strtok(NULL, " ");
                if (filename == NULL) {
                    const char* err_msg = "ERROR: DELETE requires a filename. Usage: DELETE <filename>";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    log_message("Handler: Sent ERROR (DELETE usage) to client.");
                    continue;
                }

                unsigned int index;
                Node* file_node = find_file_node(filename, &index);

                if (file_node == NULL) {
                    const char* err_msg = "ERROR: File not found.";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    sprintf(log_buffer, "Handler: Sent ERROR (File not found) to client for DELETE %s.", filename);
                    log_message(log_buffer);
                    continue;
                }

                if (strcmp(file_node->file.owner_username, client_username) != 0) {
                    pthread_mutex_unlock(&map_mutexes[index]);
                    const char* err_msg = "ERROR: You are not the owner of this file.";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    sprintf(log_buffer, "Handler: Sent ERROR (Not owner) to client for DELETE %s.", filename);
                    log_message(log_buffer);
                    continue;
                }

                StorageServer target_ss = file_node->file.location;

                pthread_mutex_unlock(&map_mutexes[index]);

                sprintf(log_buffer, "Handler: Checking SS for write locks on %s...", filename);
                log_message(log_buffer);

                int is_locked = check_if_file_is_locked_on_ss(target_ss, filename);

                if (is_locked) {
                    const char* err_msg = "ERROR: File is currently being written to. Cannot delete.";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    sprintf(log_buffer, "Handler: Sent ERROR (File locked) for DELETE %s.", filename);
                    log_message(log_buffer);
                    continue;
                }

                sprintf(log_buffer, "Handler: Proxying DELETE to SS at %s:%d", target_ss.ip, target_ss.port);
                log_message(log_buffer);
                printf("Handler: Proxying DELETE to SS at %s:%d\n", target_ss.ip, target_ss.port);

                int success = proxy_delete_command_to_ss(target_ss.ip, target_ss.port, command_buffer);

                if (success) {
                    pthread_mutex_lock(&map_mutexes[index]);

                    Node* current = file_hash_map[index];
                    Node* prev = NULL;
                    while (current != NULL) {
                        if (strcmp(current->file.filename, filename) == 0) {
                            if (prev == NULL) {
                                file_hash_map[index] = current->next;
                            } else {
                                prev->next = current->next;
                            }
                            free(current);

                            sprintf(log_buffer, "Handler: File '%s' removed from metadata map.", filename);
                            log_message(log_buffer);
                            printf("Handler: File '%s' removed from metadata map.\n", filename);
                            break;
                        }
                        prev = current;
                        current = current->next;
                    }
                    pthread_mutex_unlock(&map_mutexes[index]);

                    cache_invalidate(filename);
                    save_metadata_to_disk();

                    char ok_msg[BUFFER_SIZE];
                    sprintf(ok_msg, "File '%s' deleted successfully!", filename);
                    send(client_socket, ok_msg, strlen(ok_msg), 0);
                } else {
                    const char *err_msg = "ERROR: Storage Server failed to delete file.";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    log_message("Handler: Sent ERROR (SS delete failed) to client.");
                }

            } else if (strncmp(command_buffer, "READ ", 5) == 0) {
                printf("Handler: Received READ from '%s'\n", client_username);
                char temp_cmd[BUFFER_SIZE];
                strcpy(temp_cmd, command_buffer);
                char *filename = strtok(temp_cmd, " ");
                filename = strtok(NULL, " ");
                if (filename == NULL) {
                    const char* err_msg = "ERROR: READ requires a filename. Usage: READ <filename>";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    log_message("Handler: Sent ERROR (READ usage) to client.");
                    continue;
                }

                FileInfo file_copy;
                int found = 0;
                if (cache_get(filename, &file_copy) == 1) {
                    log_message("Handler: Cache Hit (READ)");
                    found = 1;
                } else {
                    log_message("Handler: Cache Miss (READ)");
                    unsigned int index;
                    Node* file_node = find_file_node(filename, &index);
                    if (file_node) {
                        file_copy = file_node->file;
                        cache_put(&file_copy);
                        pthread_mutex_unlock(&map_mutexes[index]);
                        found = 1;
                    }
                }

                if (!found) {
                    const char* err_msg = "ERROR: File not found.";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    sprintf(log_buffer, "Handler: Sent ERROR (File not found) for READ %s.", filename);
                    log_message(log_buffer);
                    continue;
                }

                if (check_permission(client_username, &file_copy, 'R') == 0) {
                    const char* err_msg = "ERROR: Permission denied.";                    
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    sprintf(log_buffer, "Handler: Sent ERROR (Permission denied) for READ %s.", filename);
                    log_message(log_buffer);
                    continue;
                }
                update_access_details(filename, client_username);
                StorageServer target_ss = file_copy.location;
                // --- PART 4: FAILOVER CHECK ---
                // Quick check if Main SS is reachable
                int sock = socket(AF_INET, SOCK_STREAM, 0);
                struct sockaddr_in addr;
                addr.sin_family = AF_INET;
                addr.sin_port = htons(target_ss.port);
                inet_pton(AF_INET, target_ss.ip, &addr.sin_addr);
                
                // If connection fails and we have a backup, switch target
                if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
                    if (file_copy.has_backup) {
                        target_ss = file_copy.backup_location;
                        log_message("Handler: Main SS down. Failover to Backup SS.");
                        printf("Handler: Main SS down. Failover to Backup SS.\n");
                    }
                }
                close(sock);
                // ------------------------------advance
                char ss_addr_response[BUFFER_SIZE];
                sprintf(ss_addr_response, "%s:%d", target_ss.ip, target_ss.port);
                sprintf(log_buffer, "Handler: Sending SS address to client: %s", ss_addr_response);
                log_message(log_buffer);
                printf("Handler: Sending SS address to client: %s\n", ss_addr_response);
                send(client_socket, ss_addr_response, strlen(ss_addr_response), 0);

            } else if (strncmp(command_buffer, "STREAM ", 7) == 0) {
                printf("Handler: Received STREAM from '%s'\n", client_username);
                char temp_cmd[BUFFER_SIZE];
                strcpy(temp_cmd, command_buffer);
                char *filename = strtok(temp_cmd, " ");
                filename = strtok(NULL, " ");
                if (filename == NULL) {
                    const char* err_msg = "ERROR: STREAM requires a filename. Usage: STREAM <filename>";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    log_message("Handler: Sent ERROR (STREAM usage) to client.");
                    continue;
                }

                FileInfo file_copy;
                int found = 0;
                if (cache_get(filename, &file_copy) == 1) {
                    log_message("Handler: Cache Hit (STREAM)");
                    found = 1;
                } else {
                    log_message("Handler: Cache Miss (STREAM)");
                    unsigned int index;
                    Node* file_node = find_file_node(filename, &index);
                    if (file_node) {
                        file_copy = file_node->file;
                        cache_put(&file_copy);
                        pthread_mutex_unlock(&map_mutexes[index]);
                        found = 1;
                    }
                }

                if (!found) {
                    const char* err_msg = "ERROR: File not found.";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    sprintf(log_buffer, "Handler: Sent ERROR (File not found) for STREAM %s.", filename);
                    log_message(log_buffer);
                    continue;
                }
                if (check_permission(client_username, &file_copy, 'R') == 0) {
                    const char* err_msg = "ERROR: Permission denied.";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    sprintf(log_buffer, "Handler: Sent ERROR (Permission denied) for STREAM %s.", filename);
                    log_message(log_buffer);
                    continue;
                }

                StorageServer target_ss = file_copy.location;
                char ss_addr_response[BUFFER_SIZE];
                sprintf(ss_addr_response, "%s:%d", target_ss.ip, target_ss.port);
                sprintf(log_buffer, "Handler: Sending SS address to client: %s", ss_addr_response);
                log_message(log_buffer);
                printf("Handler: Sending SS address to client: %s\n", ss_addr_response);
                send(client_socket, ss_addr_response, strlen(ss_addr_response), 0);

            } else if (strncmp(command_buffer, "WRITE ", 6) == 0) {
                printf("Handler: Received WRITE from '%s'\n", client_username);
                char temp_cmd[BUFFER_SIZE];
                strcpy(temp_cmd, command_buffer);
                char *cmd = strtok(temp_cmd, " ");
                char *filename = strtok(NULL, " ");
                char *sent_num = strtok(NULL, " ");
                if (filename == NULL || sent_num == NULL) {
                    const char* err_msg = "ERROR: WRITE format. Usage: WRITE <file> <sentence_num>";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    log_message("Handler: Sent ERROR (WRITE usage) to client.");
                    continue;
                }

                unsigned int index;
                Node* file_node = find_file_node(filename, &index);

                if (file_node == NULL) {
                    const char* err_msg = "ERROR: File not found.";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    sprintf(log_buffer, "Handler: Sent ERROR (File not found) for WRITE %s.", filename);
                    log_message(log_buffer);
                    continue;
                }

                if (check_permission(client_username, &file_node->file, 'W') == 0) {
                     pthread_mutex_unlock(&map_mutexes[index]);
                    const char* err_msg = "ERROR: Permission denied.";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    sprintf(log_buffer, "Handler: Sent ERROR (Permission denied) for WRITE %s.", filename);
                    log_message(log_buffer);
                    continue;
                }

                // update metadata timestamp & invalidate cache
                file_node->file.last_modified_time = time(NULL);
                file_node->file.last_accessed_time = time(NULL);
                strncpy(file_node->file.last_accessed_by, client_username, BUFFER_SIZE - 1);
                cache_invalidate(filename);

                StorageServer target_ss = file_node->file.location;
                pthread_mutex_unlock(&map_mutexes[index]);

                char ss_addr_response[BUFFER_SIZE];
                sprintf(ss_addr_response, "%s:%d", target_ss.ip, target_ss.port);
                sprintf(log_buffer, "Handler: Sending SS address to client: %s", ss_addr_response);
                log_message(log_buffer);
                printf("Handler: Sending SS address to client: %s\n", ss_addr_response);
                send(client_socket, ss_addr_response, strlen(ss_addr_response), 0);

            } else if (strncmp(command_buffer, "UNDO ", 5) == 0) {
                printf("Handler: Received UNDO from '%s'\n", client_username);
                char temp_cmd[BUFFER_SIZE];
                strcpy(temp_cmd, command_buffer);
                char *cmd = strtok(temp_cmd, " ");
                char *filename = strtok(NULL, " ");
                if (filename == NULL) {
                    const char* err_msg = "ERROR: UNDO requires a filename. Usage: UNDO <filename>";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    log_message("Handler: Sent ERROR (UNDO usage) to client.");
                    continue;
                }

                unsigned int index;
                Node* file_node = find_file_node(filename, &index);

                if (file_node == NULL) {
                    const char* err_msg = "ERROR: File not found.";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    sprintf(log_buffer, "Handler: Sent ERROR (File not found) for UNDO %s.", filename);
                    log_message(log_buffer);
                    continue;
                }
                if (check_permission(client_username, &file_node->file, 'W') == 0) {
                    pthread_mutex_unlock(&map_mutexes[index]);
                    const char* err_msg = "ERROR: Permission denied.";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    sprintf(log_buffer, "Handler: Sent ERROR (Permission denied) for UNDO %s.", filename);
                    log_message(log_buffer);
                    continue;
                }

                cache_invalidate(filename);

                StorageServer target_ss = file_node->file.location;
                pthread_mutex_unlock(&map_mutexes[index]);

                sprintf(log_buffer, "Handler: Proxying UNDO to SS at %s:%d", target_ss.ip, target_ss.port);
                log_message(log_buffer);
                printf("Handler: Proxying UNDO to SS at %s:%d\n", target_ss.ip, target_ss.port);
                int success = proxy_undo_command_to_ss(target_ss.ip, target_ss.port, command_buffer);
                if (success) {
                    const char *ok_msg = "Undo Successful!";
                    send(client_socket, ok_msg, strlen(ok_msg), 0);
                } else {
                    const char *err_msg = "ERROR: Storage Server failed to undo file (no backup found?).";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    log_message("Handler: Sent ERROR (SS undo failed) to client.");
                }

            } else if (strncmp(command_buffer, "ADDACCESS ", 10) == 0) {
                printf("Handler: Received ADDACCESS from '%s'\n", client_username);
                char temp_cmd[BUFFER_SIZE];
                strcpy(temp_cmd, command_buffer);
                char* cmd = strtok(temp_cmd, " "); 
                char* perm_str = strtok(NULL, " ");  
                char* filename = strtok(NULL, " ");  
                char* target_user = strtok(NULL, " "); 

                if (perm_str == NULL || filename == NULL || target_user == NULL ||
                    (strcmp(perm_str, "-R") != 0 && strcmp(perm_str, "-W") != 0)) {
                    const char* err_msg = "ERROR: Usage: ADDACCESS <-R | -W> <filename> <username>";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    log_message("Handler: Sent ERROR (ADDACCESS usage) to client.");
                    continue;
                }
                char perm_type = perm_str[1]; // 'R' or 'W'

                unsigned int index;
                Node* file_node = find_file_node(filename, &index);

                if (file_node == NULL) {
                    pthread_mutex_unlock(&map_mutexes[index]);
                    send(client_socket, "ERROR: File not found.", 20, 0);
                    log_message("Handler: Sent ERROR (File not found) for ADDACCESS.");
                    continue;
                }

                if (strcmp(file_node->file.owner_username, client_username) != 0) {
                    pthread_mutex_unlock(&map_mutexes[index]);
                    send(client_socket, "ERROR: Only the file owner can change permissions.", 48, 0);
                    log_message("Handler: Sent ERROR (Not owner) for ADDACCESS.");
                    continue;
                }

                int user_found_index = -1;
                for (int i = 0; i < file_node->file.access_count; i++) {
                    if (strcmp(file_node->file.access_list[i].username, target_user) == 0) {
                        user_found_index = i;
                        break;
                    }
                }

                if (user_found_index != -1) {
                    file_node->file.access_list[user_found_index].access_type = perm_type;
                } else {
                    if (file_node->file.access_count < MAX_CLIENTS) {
                        strncpy(file_node->file.access_list[file_node->file.access_count].username, target_user, BUFFER_SIZE - 1);
                        file_node->file.access_list[file_node->file.access_count].username[BUFFER_SIZE - 1] = '\0';
                        file_node->file.access_list[file_node->file.access_count].access_type = perm_type;
                        file_node->file.access_count++;
                    } else {
                        pthread_mutex_unlock(&map_mutexes[index]);
                        send(client_socket, "ERROR: Access list is full.", 27, 0);
                        log_message("Handler: Sent ERROR (Access list full) for ADDACCESS.");
                        continue;
                    }
                }

                cache_invalidate(filename);
                pthread_mutex_unlock(&map_mutexes[index]);

                save_metadata_to_disk();

                send(client_socket, "Access granted successfully!", 28, 0);
                sprintf(log_buffer, "Handler: Granted %c access for %s to %s.", perm_type, filename, target_user);
                log_message(log_buffer);

            } else if (strncmp(command_buffer, "REMACCESS ", 10) == 0) {
                printf("Handler: Received REMACCESS from '%s'\n", client_username);

                char temp_cmd[BUFFER_SIZE];
                strcpy(temp_cmd, command_buffer);
                char* cmd = strtok(temp_cmd, " "); 
                char* filename = strtok(NULL, " ");  
                char* target_user = strtok(NULL, " "); 

                if (filename == NULL || target_user == NULL) {
                    const char* err_msg = "ERROR: Usage: REMACCESS <filename> <username>";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    log_message("Handler: Sent ERROR (REMACCESS usage) to client.");
                    continue;
                }

                unsigned int index;
                Node* file_node = find_file_node(filename, &index);

                if (file_node == NULL) {
                    pthread_mutex_unlock(&map_mutexes[index]);
                    send(client_socket, "ERROR: File not found.", 20, 0);
                    log_message("Handler: Sent ERROR (File not found) for REMACCESS.");
                    continue;
                }

                if (strcmp(file_node->file.owner_username, client_username) != 0) {
                    pthread_mutex_unlock(&map_mutexes[index]);
                    send(client_socket, "ERROR: Only the file owner can change permissions.", 48, 0);
                    log_message("Handler: Sent ERROR (Not owner) for REMACCESS.");
                    continue;
                }

                int user_found_index = -1;
                for (int i = 0; i < file_node->file.access_count; i++) {
                    if (strcmp(file_node->file.access_list[i].username, target_user) == 0) {
                        user_found_index = i;
                        break;
                    }
                }

                if (user_found_index != -1) {
                    for (int i = user_found_index; i < file_node->file.access_count - 1; i++) {
                        file_node->file.access_list[i] = file_node->file.access_list[i+1];
                    }
                    file_node->file.access_count--;

                    cache_invalidate(filename);
                    pthread_mutex_unlock(&map_mutexes[index]);

                    save_metadata_to_disk();

                    send(client_socket, "Access removed successfully!", 28, 0);
                    sprintf(log_buffer, "Handler: Revoked access for %s from %s.", target_user, filename);
                    log_message(log_buffer);
                } else {
                    pthread_mutex_unlock(&map_mutexes[index]);
                    send(client_socket, "ERROR: User not found in access list.", 37, 0);
                    log_message("Handler: Sent ERROR (User not in list) for REMACCESS.");
                }

            } else if (strncmp(command_buffer, "EXEC ", 5) == 0) {
                printf("Handler: Received EXEC from '%s'\n", client_username);
                
                char temp_cmd[BUFFER_SIZE];
                strcpy(temp_cmd, command_buffer);
                char *cmd = strtok(temp_cmd, " ");     
                char *filename = strtok(NULL, " ");    

                if (filename == NULL) {
                    const char* err_msg = "ERROR: EXEC requires a filename. Usage: EXEC <filename>";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    log_message("Handler: Sent ERROR (EXEC usage) to client.");
                    continue;
                }
                
                FileInfo file_copy;
                int found = 0;
                if (cache_get(filename, &file_copy) == 1) {
                    log_message("Handler: Cache Hit (EXEC)");
                    found = 1;
                } else {
                    log_message("Handler: Cache Miss (EXEC)");
                    unsigned int index;
                    Node* file_node = find_file_node(filename, &index);
                    if (file_node) {
                        file_copy = file_node->file;
                        cache_put(&file_copy);
                        pthread_mutex_unlock(&map_mutexes[index]);
                        found = 1;
                    }
                }

                if (!found) {
                    const char* err_msg = "ERROR: File not found.";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    sprintf(log_buffer, "Handler: Sent ERROR (File not found) for EXEC %s.", filename);
                    log_message(log_buffer);
                    continue;
                }
                if (check_permission(client_username, &file_copy, 'R') == 0) {
                    const char* err_msg = "ERROR: Permission denied.";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    sprintf(log_buffer, "Handler: Sent ERROR (Permission denied) for EXEC %s.", filename);
                    log_message(log_buffer);
                    continue;
                }
                StorageServer target_ss = file_copy.location;
                
                printf("Handler: Fetching file content for EXEC...\n");
                log_message("Handler: Fetching file content for EXEC...");
                char file_content[MAX_FILE_SIZE];
                memset(file_content, 0, MAX_FILE_SIZE);
                
                if (fetch_file_content_from_ss(target_ss, filename, file_content, MAX_FILE_SIZE) == 0) {
                    const char* err_msg = "ERROR: Failed to fetch file from Storage Server.";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    log_message("Handler: Sent ERROR (SS fetch failed) for EXEC.");
                    continue;
                }
                
                snprintf(log_buffer, sizeof(log_buffer), "Handler: Executing content: %.1024s...", file_content);
                log_message(log_buffer);
                printf("Handler: Executing content: %s\n", file_content);
                FILE *command_pipe = popen(file_content, "r");
                if (command_pipe == NULL) {
                    const char* err_msg = "ERROR: NM failed to execute command (popen failed).";
                    send(client_socket, err_msg, strlen(err_msg), 0);
                    log_message("Handler: Sent ERROR (popen failed) for EXEC.");
                    continue;
                }
                
                char chunk_buffer[BUFFER_SIZE];
                while (fgets(chunk_buffer, sizeof(chunk_buffer), command_pipe) != NULL) {
                    if (send(client_socket, chunk_buffer, strlen(chunk_buffer), 0) < 0) {
                        perror("Handler: send (popen) failed");
                        break; 
                    }
                }
                
                pclose(command_pipe);
                
                send(client_socket, EXEC_TERMINATOR, strlen(EXEC_TERMINATOR), 0);
                log_message("Handler: Sent EXEC_TERMINATOR to client.");
                // --- PART 1: CREATEFOLDER ---advance
            } else if (strncmp(command_buffer, "CREATEFOLDER ", 13) == 0) {
                printf("Handler: Received CREATEFOLDER from '%s'\n", client_username);
                char temp_cmd[BUFFER_SIZE];
                strcpy(temp_cmd, command_buffer);
                char *cmd = strtok(temp_cmd, " ");
                char *foldername = strtok(NULL, " ");

                if (foldername == NULL) {
                    send(client_socket, "ERROR: Usage: CREATEFOLDER <foldername>", 39, 0);
                    continue;
                }

                unsigned int index;
                Node* node = find_file_node(foldername, &index); // Checks if name taken
                if (node != NULL) {
                    pthread_mutex_unlock(&map_mutexes[index]);
                    send(client_socket, "ERROR: Name already exists.", 27, 0);
                    continue;
                }

                // Create Logical Folder Node
                Node* new_node = (Node*)malloc(sizeof(Node));
                memset(&new_node->file, 0, sizeof(FileInfo));
                strcpy(new_node->file.filename, foldername);
                strcpy(new_node->file.owner_username, client_username);
                new_node->file.is_directory = 1; 
                strcpy(new_node->file.parent_dir, "root"); // Folders created at root by default
                
                // Link into hash map
                pthread_mutex_lock(&map_mutexes[index]);
                new_node->next = file_hash_map[index];
                file_hash_map[index] = new_node;
                pthread_mutex_unlock(&map_mutexes[index]);
                
                save_metadata_to_disk();
                send(client_socket, "Folder created successfully!", 28, 0);
                sprintf(log_buffer, "Handler: User '%s' created folder '%s'", client_username, foldername);
                log_message(log_buffer);

            // --- PART 1: MOVE ---
            } else if (strncmp(command_buffer, "MOVE ", 5) == 0) {
                printf("Handler: Received MOVE from '%s'\n", client_username);
                char temp_cmd[BUFFER_SIZE];
                strcpy(temp_cmd, command_buffer);
                char *cmd = strtok(temp_cmd, " ");
                char *filename = strtok(NULL, " ");
                char *dest_folder = strtok(NULL, " ");

                if (filename == NULL || dest_folder == NULL) {
                    send(client_socket, "ERROR: Usage: MOVE <filename> <foldername>", 42, 0);
                    continue;
                }

                // 1. Check if Destination Folder Exists
                unsigned int folder_idx;
                Node* folder_node = find_file_node(dest_folder, &folder_idx);
                if (folder_node == NULL) {
                    send(client_socket, "ERROR: Destination folder not found.", 36, 0);
                    continue;
                }
                if (folder_node->file.is_directory == 0) {
                    pthread_mutex_unlock(&map_mutexes[folder_idx]);
                    send(client_socket, "ERROR: Destination is not a folder.", 35, 0);
                    continue;
                }
                pthread_mutex_unlock(&map_mutexes[folder_idx]); // Unlock folder bucket

                // 2. Find the File to Move
                unsigned int file_idx;
                Node* file_node = find_file_node(filename, &file_idx);
                if (file_node == NULL) {
                    send(client_socket, "ERROR: File to move not found.", 30, 0);
                    continue;
                }

                // 3. Perform Move
                strcpy(file_node->file.parent_dir, dest_folder);
                pthread_mutex_unlock(&map_mutexes[file_idx]); // Unlock file bucket

                save_metadata_to_disk();
                send(client_socket, "File moved successfully!", 24, 0);
                sprintf(log_buffer, "Handler: Moved '%s' to '%s'", filename, dest_folder);
                log_message(log_buffer);

             // --- PART 1: VIEWFOLDER ---
            } else {
                printf("Handler: Unknown command: %s\n", command_buffer);
                const char *unknown_msg = "ERROR: Unknown command. Try LIST, VIEW, INFO, CREATE, DELETE, READ, STREAM, WRITE, UNDO, ADDACCESS, REMACCESS, or EXIT.\n";
                send(client_socket, unknown_msg, strlen(unknown_msg), 0);
                log_message("Handler: Sent ERROR (Unknown command) to client.");
            }
        } // end while commands
    } // end if is_user_client

    // --- 4. Cleanup ---
    if (is_user_client) {
        pthread_mutex_lock(&list_mutex);
        int user_index = -1;
        for (int i = 0; i < online_count; i++) {
            if (strcmp(online_users[i].username, client_username) == 0) {
                user_index = i;
                break;
            }
        }
        if (user_index != -1) {
            for (int i = user_index; i < online_count - 1; i++) {
                strcpy(online_users[i].username, online_users[i+1].username);
            }
            online_count--;
            sprintf(log_buffer, "Handler: Removed '%s' from *online* list. Total online: %d", client_username, online_count);
            log_message(log_buffer);
            printf("Handler: Removed '%s' from *online* list. Total online: %d\n", client_username, online_count);
        }
        pthread_mutex_unlock(&list_mutex);
    }
    
    close(client_socket);
    sprintf(log_buffer, "Handler: Client %s:%d disconnected, thread closing.",
           inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
    log_message(log_buffer);
    printf("Handler: Client %s:%d disconnected, thread closing.\n",
           inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
    return NULL;
}

// --- Main function ---
int main() {
    int server_fd;
    struct sockaddr_in server_addr;

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 10) < 0) {
        perror("listen failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    // --- Init Mutexes ---
    if (pthread_mutex_init(&list_mutex, NULL) != 0) {
        perror("list_mutex_init failed");
        exit(EXIT_FAILURE);
    }
    if (pthread_mutex_init(&log_mutex, NULL) != 0) {
        perror("log_mutex_init failed");
        exit(EXIT_FAILURE);
    }
    if (pthread_mutex_init(&cache_mutex, NULL) != 0) { 
        perror("cache_mutex_init failed");
        exit(EXIT_FAILURE);
    }
    
    for (int i = 0; i < HASH_MAP_SIZE; i++) {
        if (pthread_mutex_init(&map_mutexes[i], NULL) != 0) {
            perror("map_mutex_init failed");
            exit(EXIT_FAILURE);
        }
        file_hash_map[i] = NULL; 
    }
    
    // --- Load persistent data ---
    load_metadata_from_disk();
    load_users_from_disk(); // NEW
    
    char start_log[100];
    sprintf(start_log, "Name Server started. Listening on port %d...", PORT);
    log_message(start_log);
    printf("Name Server is listening on port %d...\n", PORT);

    while (1) {
        int new_socket;
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        new_socket = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        if (new_socket < 0) {
            perror("accept failed");
            continue; 
        }
        
        char log_buf[100];
        sprintf(log_buf, "Main: New connection from %s:%d",
               inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
        log_message(log_buf);
        printf("Main: New connection from %s:%d\n",
               inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

        pthread_t thread_id;
        if (pthread_create(&thread_id, NULL, handle_client, (void*)(intptr_t)new_socket) != 0) {
            perror("pthread_create failed");
            log_message("Main: pthread_create failed.");
           
           
            close(new_socket);
        } else {
            pthread_detach(thread_id);
        }
    }

    // --- Destroy all mutexes ---
    pthread_mutex_destroy(&list_mutex);
    pthread_mutex_destroy(&log_mutex);
    pthread_mutex_destroy(&cache_mutex); 
    for (int i = 0; i < HASH_MAP_SIZE; i++) {
        pthread_mutex_destroy(&map_mutexes[i]);
    }

    // Free all nodes in hash map
    for (int i = 0; i < HASH_MAP_SIZE; i++) {
        Node* current = file_hash_map[i];
        while (current != NULL) {
            Node* temp = current;
            current = current->next;
            free(temp);
        }
    }
    
    // Free all nodes in cache
    CacheNode* current_cache = lru_cache_head;
    while (current_cache != NULL) {
        CacheNode* temp = current_cache;
        current_cache = current_cache->next;
        free(temp);
    }


    close(server_fd);
    return 0;
}