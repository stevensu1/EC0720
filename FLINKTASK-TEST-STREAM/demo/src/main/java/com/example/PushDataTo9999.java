package com.example;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PushDataTo9999 {
    private static final int PORT = 9999;
    private static final String DATA = "test_flink_window_hallo_word";

    public static void main(String[] args) {
        ExecutorService executor = Executors.newCachedThreadPool();

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Server started on port " + PORT);

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    // 等待客户端连接
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Client connected: " + clientSocket.getRemoteSocketAddress());

                    // 为每个客户端启动一个独立线程处理数据推送
                    executor.submit(() -> {
                        try (OutputStream outputStream = clientSocket.getOutputStream()) {
                            while (!Thread.currentThread().isInterrupted() && !clientSocket.isClosed()) {
                                // 获取当前系统时间
                                String currentTime = LocalDateTime.now()
                                        .format(DateTimeFormatter.ofPattern("HH:mm:ss"));

                                // 每秒发送一次带时间戳的数据
                                String dataToSend = DATA + " " + currentTime + "\n";
                                outputStream.write(dataToSend.getBytes(StandardCharsets.UTF_8));
                                outputStream.flush();
                                System.out.println("Sent: " + DATA);

                                // 等待1秒
                                Thread.sleep(1000);
                            }
                        } catch (IOException | InterruptedException e) {
                            System.err.println("Error sending data to client: " + e.getMessage());
                        } finally {
                            try {
                                clientSocket.close();
                            } catch (IOException e) {
                                System.err.println("Error closing client socket: " + e.getMessage());
                            }
                        }
                    });
                } catch (IOException e) {
                    System.err.println("Error accepting client connection: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Error starting server on port " + PORT + ": " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }
}
