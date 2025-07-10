# 📧 Setup Email Notifications cho Airflow

## 1. Cấu hình Gmail App Password

### Bước 1: Bật 2-Factor Authentication
1. Vào [Google Account](https://myaccount.google.com/)
2. Chọn **Security** → **2-Step Verification**
3. Bật 2-Step Verification nếu chưa có

### Bước 2: Tạo App Password
1. Vào **Security** → **App passwords**
2. Chọn **Mail** và **Other (custom name)**
3. Nhập tên: `Airflow Notifications`
4. Copy mật khẩu 16 ký tự được tạo

## 2. Cấu hình .env file

Tạo file `.env` trong thư mục gốc:

```env
# SMTP Configuration for Airflow Email Notifications
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-16-character-app-password

# Airflow UID (để tránh permission issues)
AIRFLOW_UID=50000
```

## 3. Restart Docker Services

```bash
docker-compose down
docker-compose up -d
```

## 4. Test Email Notifications

1. Trigger một DAG
2. Để DAG fail hoặc retry
3. Kiểm tra email inbox

## 5. Cấu hình Email trong DAG

```python
default_args = {
    'email': ['your-email@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': False,
}
```

## 6. Troubleshooting

### Lỗi "Authentication failed"
- Kiểm tra App Password đã đúng chưa
- Đảm bảo 2FA đã bật

### Lỗi "Connection refused"
- Kiểm tra firewall/network
- Thử port 465 với SSL thay vì 587

### Test SMTP từ Airflow UI
1. Vào Admin → Configuration
2. Tìm `smtp` để xem config
3. Hoặc vào Admin → Connections để test

## 7. Email Templates

Airflow sẽ tự động gửi email với:
- Task failure details
- DAG run information
- Log snippets
- Retry information

Bạn có thể customize email templates trong `airflow/config/` nếu cần. 