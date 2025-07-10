import time
import sys
import os

# Đảm bảo import được generate_sample_data.py nếu chạy trực tiếp từ scripts/
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

import generate_sample_data

def main():
    print("[Auto] Bắt đầu auto generate sample data mỗi 1 phút...")
    while True:
        print("\n[Auto] Gọi generate_sample_data...")
        try:
            generate_sample_data.main()  # Giả sử file gốc có hàm main()
        except Exception as e:
            print(f"[Auto] Lỗi khi generate sample data: {e}")
        print("[Auto] Đợi 60s...")
        time.sleep(10)

if __name__ == "__main__":
    main() 