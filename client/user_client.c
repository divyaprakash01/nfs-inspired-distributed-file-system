/*
 * user_client.c - Final Complete Version
 * Supports: READ, WRITE, STREAM, EXEC, INFO, LIST, VIEW,
 * CHECKPOINT, REVERT, VIEWCHECKPOINT,
 * REQACCESS, VIEWREQ, APPROVEREQ, REJECTREQ,
 * CREATEFOLDER, MOVE, VIEWFOLDER
 */

#include <stdio.h>      // For printf, perror, fgets
#include <stdlib.h>     // For exit(), atoi()
#include <string.h>     // For memset(), strlen(), strcspn(), strncmp(), strtok()
#include <unistd.h>     // For close(), write()
#include <arpa/inet.h>  // For inet_pton()
#include <sys/socket.h> // For socket(), connect(), send(), recv()

#define SERVER_IP "127.0.0.1" // The IP of the Name Server
#define SERVER_PORT 8080      // The Port of the Name Server
#define BUFFER_SIZE 1024
#define RESPONSE_SIZE 4096    // Increased buffer for large lists
#define EXEC_TERMINATOR "__EXEC_COMPLETE__" 

/*
 * Helper function to connect to a new server (the SS)
 */
int connect_to_storage_server(const char* ss_ip, int ss_port) {
    int ss_socket;
    struct sockaddr_in ss_addr;

    ss_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (ss_socket < 0) {
        perror("Client: Failed to create SS socket");
        return -1;
    }

    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_port = htons(ss_port);
    if (inet_pton(AF_INET, ss_ip, &ss_addr.sin_addr) <= 0) {
        perror("Client: Invalid SS address");
        close(ss_socket);
        return -1;
    }

    if (connect(ss_socket, (struct sockaddr *)&ss_addr, sizeof(ss_addr)) < 0) {
        perror("Client: Failed to connect to SS");
        close(ss_socket);
        return -1;
    }

    return ss_socket;
}

int main() {
    int sock_fd; 
    struct sockaddr_in server_addr;
    char username[BUFFER_SIZE];
    char command_buffer[BUFFER_SIZE];
    char response_buffer[RESPONSE_SIZE];

    printf("Enter username: ");
    if (fgets(username, BUFFER_SIZE, stdin) == NULL) {
        perror("fgets failed");
        exit(EXIT_FAILURE);
    }
    username[strcspn(username, "\n")] = '\0'; 

    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    if (inet_pton(AF_INET, SERVER_IP, &server_addr.sin_addr) <= 0) {
        perror("invalid address");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }
    if (connect(sock_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect failed");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }

    if (send(sock_fd, username, strlen(username), 0) < 0) {
        perror("send failed");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }

    printf("Connected and registered as user: '%s'\n", username);
    printf("Type commands (CREATE, READ, WRITE, INFO, REQACCESS, etc...) or EXIT to quit.\n");

    while (1) {
        printf("> ");
        fflush(stdout); 

        if (fgets(command_buffer, BUFFER_SIZE, stdin) == NULL) {
            printf("\nDisconnecting...\n");
            send(sock_fd, "EXIT", 4, 0); 
            break; 
        }
        
        command_buffer[strcspn(command_buffer, "\n")] = '\0';

        if (strlen(command_buffer) == 0) continue;

        if (strncmp(command_buffer, "EXIT", 4) == 0) {
            send(sock_fd, "EXIT", 4, 0);
            break; 
        
        // --- 1. Simple Command Handling (Send -> Recv Text -> Print) ---
        } else if (strncmp(command_buffer, "LIST", 4) == 0 ||
                   strncmp(command_buffer, "VIEW", 4) == 0 ||
                   strncmp(command_buffer, "INFO ", 5) == 0 ||
                   strncmp(command_buffer, "CREATE ", 7) == 0 ||
                   strncmp(command_buffer, "DELETE ", 7) == 0 ||
                   strncmp(command_buffer, "UNDO ", 5) == 0 ||
                   strncmp(command_buffer, "ADDACCESS ", 10) == 0 || 
                   strncmp(command_buffer, "REMACCESS ", 10) == 0 ||
                   strncmp(command_buffer, "CREATEFOLDER ", 13) == 0 ||
                   strncmp(command_buffer, "MOVE ", 5) == 0 ||
                   strncmp(command_buffer, "VIEWFOLDER ", 11) == 0 ||
                   strncmp(command_buffer, "CHECKPOINT ", 11) == 0 ||
                   strncmp(command_buffer, "REVERT ", 7) == 0 ||
                   strncmp(command_buffer, "VIEWCHECKPOINT ", 15) == 0 ||
                   strncmp(command_buffer, "REQACCESS ", 10) == 0 ||
                   strncmp(command_buffer, "APPROVEREQ ", 11) == 0 ||
                   strncmp(command_buffer, "REJECTREQ ", 10) == 0 ||
                   strncmp(command_buffer, "VIEWREQ", 7) == 0 ||
                   strncmp(command_buffer, "LISTCHECKPOINTS ", 16) == 0) {
            
            if (send(sock_fd, command_buffer, strlen(command_buffer), 0) < 0) {
                perror("send failed");
                break;
            }

            memset(response_buffer, 0, RESPONSE_SIZE);
            int bytes_received = recv(sock_fd, response_buffer, RESPONSE_SIZE - 1, 0);
            
            if (bytes_received <= 0) {
                printf("Server disconnected.\n");
                break;
            }
            
            // Prevent double newlines but ensure at least one
            write(STDOUT_FILENO, response_buffer, bytes_received);
            if (bytes_received > 0 && response_buffer[bytes_received - 1] != '\n') {
                printf("\n");
            }

        // --- 2. READ Handling (Connect to SS) ---
        } else if (strncmp(command_buffer, "READ ", 5) == 0) {
            if (send(sock_fd, command_buffer, strlen(command_buffer), 0) < 0) {
                perror("send failed");
                break;
            }

            memset(response_buffer, 0, RESPONSE_SIZE);
            int bytes_received = recv(sock_fd, response_buffer, RESPONSE_SIZE - 1, 0);
            if (bytes_received <= 0) {
                printf("Name Server disconnected.\n");
                break;
            }

            if (strncmp(response_buffer, "ERROR:", 6) == 0) {
                printf("%s\n", response_buffer);
                continue; 
            }

            char* ss_ip = strtok(response_buffer, ":");
            char* ss_port_str = strtok(NULL, ":");

            if (ss_ip == NULL || ss_port_str == NULL) {
                printf("Client: Received malformed address from NM: %s\n", response_buffer);
                continue;
            }
            int ss_port = atoi(ss_port_str);

            printf("Client: Connecting to Storage Server at %s:%d...\n", ss_ip, ss_port);
            int ss_socket = connect_to_storage_server(ss_ip, ss_port);
            if (ss_socket < 0) {
                printf("Client: Could not connect to Storage Server.\n");
                continue;
            }

            char read_request[BUFFER_SIZE];
            char* filename = command_buffer + 5; 
            sprintf(read_request, "REQUEST_READ %s", filename);

            if (send(ss_socket, read_request, strlen(read_request), 0) < 0) {
                perror("Client: Failed to send request to SS");
                close(ss_socket);
                continue;
            }

            printf("--- Start of file '%s' ---\n", filename);
            memset(response_buffer, 0, RESPONSE_SIZE);
            while ((bytes_received = recv(ss_socket, response_buffer, RESPONSE_SIZE - 1, 0)) > 0) {
                write(STDOUT_FILENO, response_buffer, bytes_received);
                memset(response_buffer, 0, RESPONSE_SIZE);
            }
            
            printf("\n--- End of file '%s' ---\n", filename);
            close(ss_socket);

        // --- 3. STREAM Handling (Connect to SS) ---
        } else if (strncmp(command_buffer, "STREAM ", 7) == 0) {
            if (send(sock_fd, command_buffer, strlen(command_buffer), 0) < 0) {
                perror("send failed");
                break;
            }

            memset(response_buffer, 0, RESPONSE_SIZE);
            int bytes_received = recv(sock_fd, response_buffer, RESPONSE_SIZE - 1, 0);
            if (bytes_received <= 0) {
                printf("Name Server disconnected.\n");
                break;
            }

            if (strncmp(response_buffer, "ERROR:", 6) == 0) {
                printf("%s\n", response_buffer);
                continue; 
            }

            char* ss_ip = strtok(response_buffer, ":");
            char* ss_port_str = strtok(NULL, ":");
            if (ss_ip == NULL || ss_port_str == NULL) {
                printf("Client: Received malformed address from NM.\n");
                continue;
            }
            int ss_port = atoi(ss_port_str);

            printf("Client: Connecting to Storage Server at %s:%d...\n", ss_ip, ss_port);
            int ss_socket = connect_to_storage_server(ss_ip, ss_port);
            if (ss_socket < 0) {
                printf("Client: Could not connect to Storage Server.\n");
                continue;
            }

            char stream_request[BUFFER_SIZE];
            char* filename = command_buffer + 7; 
            sprintf(stream_request, "REQUEST_STREAM %s", filename);

            if (send(ss_socket, stream_request, strlen(stream_request), 0) < 0) {
                perror("Client: Failed to send request to SS");
                close(ss_socket);
                continue;
            }

            printf("--- Start of stream '%s' ---\n", filename);
            fflush(stdout); 
            
            memset(response_buffer, 0, RESPONSE_SIZE);
            while ((bytes_received = recv(ss_socket, response_buffer, RESPONSE_SIZE - 1, 0)) > 0) {
                write(STDOUT_FILENO, response_buffer, bytes_received);
                fflush(stdout); 
                memset(response_buffer, 0, RESPONSE_SIZE);
            }
            printf("\n--- End of stream '%s' ---\n", filename);
            close(ss_socket);

        // --- 4. WRITE Handling (Connect to SS) ---
        } else if (strncmp(command_buffer, "WRITE ", 6) == 0) {
            if (send(sock_fd, command_buffer, strlen(command_buffer), 0) < 0) {
                perror("send failed");
                break;
            }

            memset(response_buffer, 0, RESPONSE_SIZE);
            int bytes_received = recv(sock_fd, response_buffer, RESPONSE_SIZE - 1, 0);
            if (bytes_received <= 0) {
                printf("Name Server disconnected.\n");
                break;
            }

            if (strncmp(response_buffer, "ERROR:", 6) == 0) {
                printf("%s\n", response_buffer);
                continue; 
            }

            char* ss_ip = strtok(response_buffer, ":");
            char* ss_port_str = strtok(NULL, ":");
            if (ss_ip == NULL || ss_port_str == NULL) {
                printf("Client: Received malformed address from NM.\n");
                continue;
            }
            int ss_port = atoi(ss_port_str);

            printf("Client: Connecting to Storage Server at %s:%d...\n", ss_ip, ss_port);
            int ss_socket = connect_to_storage_server(ss_ip, ss_port);
            if (ss_socket < 0) {
                printf("Client: Could not connect to Storage Server.\n");
                continue;
            }

            char write_request[BUFFER_SIZE];
            char* write_args = command_buffer + 6; 
            sprintf(write_request, "REQUEST_WRITE %s", write_args);

            if (send(ss_socket, write_request, strlen(write_request), 0) < 0) {
                perror("Client: Failed to send request to SS");
                close(ss_socket);
                continue;
            }

            memset(response_buffer, 0, RESPONSE_SIZE);
            bytes_received = recv(ss_socket, response_buffer, RESPONSE_SIZE - 1, 0);
            if (bytes_received <= 0) {
                printf("Storage Server disconnected.\n");
                close(ss_socket);
                continue;
            }

            if (strncmp(response_buffer, "ACK_LOCKED", 10) == 0) {
                printf("Sentence locked. Enter write commands (e.g., '1 new_word') or 'ETIRW' to finish.\n");
                
                char write_op_buffer[BUFFER_SIZE];
                while(1) {
                    printf("write> ");
                    fflush(stdout);

                    if (fgets(write_op_buffer, BUFFER_SIZE, stdin) == NULL) {
                        strcpy(write_op_buffer, "ETIRW\n");
                    }
                    write_op_buffer[strcspn(write_op_buffer, "\n")] = '\0';
                    
                    if (send(ss_socket, write_op_buffer, strlen(write_op_buffer), 0) < 0) {
                        perror("Client: Failed to send write op to SS");
                        break;
                    }

                    memset(response_buffer, 0, RESPONSE_SIZE);
                    bytes_received = recv(ss_socket, response_buffer, RESPONSE_SIZE - 1, 0);
                    if (bytes_received <= 0) {
                        printf("Storage Server disconnected during write.\n");
                        break;
                    }

                    printf("%s\n", response_buffer); 

                    if (strncmp(response_buffer, "ACK_WRITE_COMPLETE", 18) == 0) {
                        break; 
                    }
                }
            } else {
                printf("%s\n", response_buffer);
            }
            close(ss_socket);

        // --- 5. EXEC Handling (Stream Output) ---
        } else if (strncmp(command_buffer, "EXEC ", 5) == 0) {
            if (send(sock_fd, command_buffer, strlen(command_buffer), 0) < 0) {
                perror("send failed");
                break;
            }
            
            printf("--- Start of execution output ---\n");
            fflush(stdout);

            while(1) {
                memset(response_buffer, 0, RESPONSE_SIZE);
                int bytes_received = recv(sock_fd, response_buffer, RESPONSE_SIZE - 1, 0);

                if (bytes_received <= 0) {
                    printf("\nServer disconnected or stream ended unexpectedly.\n");
                    break;
                }

                if (strstr(response_buffer, EXEC_TERMINATOR) != NULL) {
                    char* terminator_pos = strstr(response_buffer, EXEC_TERMINATOR);
                    write(STDOUT_FILENO, response_buffer, terminator_pos - response_buffer);
                    fflush(stdout);
                    break; 
                }

                write(STDOUT_FILENO, response_buffer, bytes_received);
                fflush(stdout);
            }
            printf("\n--- End of execution output ---\n");
        
        // --- 6. Fallback / Catch-all ---
        } else if (strlen(command_buffer) > 0) {
             // Try sending unknown commands to see if server has a message for them
            if (send(sock_fd, command_buffer, strlen(command_buffer), 0) < 0) {
                perror("send failed");
                break;
            }
            memset(response_buffer, 0, RESPONSE_SIZE);
            int bytes_received = recv(sock_fd, response_buffer, RESPONSE_SIZE - 1, 0);
            if (bytes_received > 0) {
                printf("%s\n", response_buffer);
            }
        }
    }

    close(sock_fd);
    printf("Disconnected from server.\n");
    return 0;
}