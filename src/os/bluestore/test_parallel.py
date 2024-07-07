import sys


def parse_log(file_path):
    numbers = []
    with open(file_path, 'r') as file:
        for line in file:
            if 'mydebug: committing' in line:
                number = line.split(' ')[-1].strip()
                numbers.append(int(number))
    return numbers

def calculate_average(numbers):
    if not numbers:
        return 0
    return sum(numbers) / len(numbers)

def main():
    file_path = '/var/log/ceph/ceph-osd.0.log'
    numbers = parse_log(file_path)
    average = calculate_average(numbers)
    print("Numbers:", numbers)
    print("Average:", average)

if __name__ == "__main__":
    main()