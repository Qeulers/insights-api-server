# Server-Sent Events (SSE) Notification System

## Overview

This document describes the real-time notification system implemented using Server-Sent Events (SSE) for the Insights API server. The system allows frontend clients to receive real-time zone/port notifications as they are processed by the webhook system.

## Architecture

### Components

1. **SSEConnectionManager**: Manages active SSE connections, handles broadcasting, and maintains connection health
2. **SSE Endpoint**: `/notifications/zone-port-events/stream` - The main SSE endpoint for client connections
3. **Broadcasting Integration**: Automatic notification broadcasting after webhook processing and screening
4. **Connection Management**: Handles connect/disconnect, heartbeats, timeouts, and graceful cleanup

### Key Features

- **Real-time Broadcasting**: Notifications are sent to connected clients immediately after processing
- **Connection Management**: Tracks active connections with automatic cleanup
- **Heartbeat System**: Periodic heartbeats (30s intervals) to maintain connections
- **Security**: XSS prevention through HTML sanitization, user authentication
- **Error Handling**: Comprehensive error handling with exponential backoff recommendations
- **Production Ready**: Connection limits, timeouts, logging, and memory management

## API Endpoint

### GET `/notifications/zone-port-events/stream`

**Description**: Establishes an SSE connection for real-time zone/port notifications.

**Parameters**:
- `user_id` (required): User ID for authentication

**Headers**:
- `Cache-Control: no-cache`
- `Connection: keep-alive`
- `Content-Type: text/event-stream`
- `Access-Control-Allow-Origin: *`
- `X-Accel-Buffering: no`

**Response**: Streaming response with SSE events

## Message Types

### 1. Connection Established
```json
{
  "type": "connection_established",
  "user_id": "user123",
  "timestamp": "2024-01-01T12:00:00Z",
  "message": "SSE connection established successfully"
}
```

### 2. Heartbeat
```json
{
  "type": "heartbeat",
  "timestamp": "2024-01-01T12:00:30Z"
}
```

### 3. Zone/Port Notification
```json
{
  "type": "zone_port_notification",
  "notification_id": "507f1f77bcf86cd799439011",
  "timestamp": "2024-01-01T12:01:00Z",
  "user_id": "user123",
  "data": {
    // Complete notification data including screening results
    "vessel": { ... },
    "event": { ... },
    "screening_results": { ... },
    "received_at": "2024-01-01T12:00:45Z",
    "auto_screen": true
  }
}
```

## Client Implementation

### JavaScript Example

```javascript
class NotificationClient {
    constructor(userId, serverUrl = 'http://localhost:8000') {
        this.userId = userId;
        this.serverUrl = serverUrl;
        this.eventSource = null;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 5;
        this.reconnectDelay = 1000; // Start with 1 second
    }

    connect() {
        const url = `${this.serverUrl}/notifications/zone-port-events/stream?user_id=${encodeURIComponent(this.userId)}`;
        
        this.eventSource = new EventSource(url);
        
        this.eventSource.onopen = () => {
            console.log('SSE connection established');
            this.reconnectAttempts = 0;
            this.reconnectDelay = 1000;
        };
        
        this.eventSource.onmessage = (event) => {
            const data = JSON.parse(event.data);
            this.handleMessage(data);
        };
        
        this.eventSource.onerror = (event) => {
            console.error('SSE error:', event);
            this.handleReconnection();
        };
    }

    handleMessage(data) {
        switch (data.type) {
            case 'connection_established':
                console.log('Connection confirmed for user:', data.user_id);
                break;
            case 'heartbeat':
                console.log('Heartbeat received');
                break;
            case 'zone_port_notification':
                this.onNotification(data);
                break;
            default:
                console.log('Unknown message type:', data);
        }
    }

    onNotification(notification) {
        // Override this method to handle notifications
        console.log('New notification:', notification);
    }

    handleReconnection() {
        if (this.reconnectAttempts < this.maxReconnectAttempts) {
            this.reconnectAttempts++;
            const delay = Math.min(
                this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1),
                30000 // Max 30 seconds
            );
            
            console.log(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`);
            
            setTimeout(() => {
                if (this.eventSource.readyState === EventSource.CLOSED) {
                    this.connect();
                }
            }, delay);
        } else {
            console.error('Max reconnection attempts reached');
            this.disconnect();
        }
    }

    disconnect() {
        if (this.eventSource) {
            this.eventSource.close();
            this.eventSource = null;
        }
    }
}

// Usage
const client = new NotificationClient('your_user_id');
client.onNotification = (notification) => {
    // Handle notification in your application
    console.log('Received notification:', notification.data);
};
client.connect();
```

### React Hook Example

```javascript
import { useEffect, useRef, useState } from 'react';

export function useSSENotifications(userId, serverUrl = 'http://localhost:8000') {
    const [notifications, setNotifications] = useState([]);
    const [connectionStatus, setConnectionStatus] = useState('disconnected');
    const eventSourceRef = useRef(null);

    useEffect(() => {
        if (!userId) return;

        const url = `${serverUrl}/notifications/zone-port-events/stream?user_id=${encodeURIComponent(userId)}`;
        
        setConnectionStatus('connecting');
        eventSourceRef.current = new EventSource(url);

        eventSourceRef.current.onopen = () => {
            setConnectionStatus('connected');
        };

        eventSourceRef.current.onmessage = (event) => {
            const data = JSON.parse(event.data);
            
            if (data.type === 'zone_port_notification') {
                setNotifications(prev => [data, ...prev]);
            }
        };

        eventSourceRef.current.onerror = () => {
            setConnectionStatus('error');
        };

        return () => {
            if (eventSourceRef.current) {
                eventSourceRef.current.close();
            }
        };
    }, [userId, serverUrl]);

    return { notifications, connectionStatus };
}
```

## Configuration

### Connection Limits
- **Max Connections**: 100 concurrent connections
- **Queue Size**: 50 messages per connection
- **Connection Timeout**: 5 minutes (300 seconds)
- **Heartbeat Interval**: 30 seconds

### Security Features
- **Authentication**: User ID validation through existing auth system
- **XSS Prevention**: HTML sanitization of all notification content
- **CORS Support**: Configured for development environments
- **Rate Limiting**: Built-in connection limits

## Monitoring and Logging

### Log Categories
- **SSE Logger**: Connection events, broadcasting, errors
- **Notifications Logger**: Webhook processing, screening updates

### Key Metrics to Monitor
- Active connection count
- Message broadcast success rate
- Connection duration
- Reconnection frequency
- Queue overflow events

### Log Examples
```
[INFO] sse: New SSE connection established. Active connections: 5
[INFO] sse: Broadcasting notification 507f1f77bcf86cd799439011 to 5 connections
[WARNING] sse: Connection queue full, removing stale connection
[ERROR] sse: SSE stream error for user user123: Connection reset by peer
```

## Testing

### Using the Test Client
1. Open `sse_test_client.html` in a web browser
2. Enter your user ID and server URL
3. Click "Connect" to establish SSE connection
4. Trigger notifications through webhook endpoints
5. Monitor real-time message delivery

### Manual Testing with curl
```bash
# Test SSE endpoint
curl -N -H "Accept: text/event-stream" \
  "http://localhost:8000/notifications/zone-port-events/stream?user_id=test_user"

# Trigger notification via webhook
curl -X POST "http://localhost:8000/notifications/webhook/zone-port-event" \
  -H "Content-Type: application/json" \
  -d '{
    "custom_reference": "test_user|true",
    "vessel": {"imo": "1234567"},
    "event": {"type": "zone_entry"}
  }'
```

## Deployment Considerations

### Production Settings
- Configure appropriate CORS origins (not `*`)
- Set up proper SSL/TLS certificates
- Monitor connection counts and memory usage
- Configure reverse proxy buffering (nginx: `proxy_buffering off`)

### Scaling
- Consider Redis for multi-instance broadcasting
- Monitor memory usage with many concurrent connections
- Implement connection pooling for database operations

### Error Handling
- Client should implement exponential backoff
- Server automatically cleans up stale connections
- Comprehensive logging for debugging

## Troubleshooting

### Common Issues

1. **Connection Rejected**: Check user authentication
2. **No Messages Received**: Verify webhook is triggering notifications
3. **Frequent Disconnections**: Check network stability, implement proper reconnection
4. **Memory Issues**: Monitor connection count and queue sizes

### Debug Steps
1. Check server logs for SSE and notification events
2. Use test client to verify connection establishment
3. Trigger test notifications manually
4. Monitor network traffic for SSE messages

## Future Enhancements

- **User-specific filtering**: Only send notifications relevant to specific users
- **Message persistence**: Store missed messages for offline clients
- **WebSocket alternative**: Implement WebSocket support for bidirectional communication
- **Metrics dashboard**: Real-time monitoring of SSE system health
- **Message compression**: Reduce bandwidth usage for large notifications
