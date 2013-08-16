import argparse
import csv


def main():
    parser = argparse.ArgumentParser(
        description='Split "coaches" field into separate rows')
    parser.add_argument('input_file', type=str,
                        help='the file to process')
    input_filename = parser.parse_args().input_file
    explode(input_filename)


def explode(filename):
    with open(filename, 'rb') as oldfile:
        with open('exploded.csv', 'wb') as newfile:
            reader = csv.reader(oldfile, delimiter=',', quotechar='"', 
                skipinitialspace=True)
            writer = csv.writer(newfile, delimiter=',',
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                if "," in row[1]:
                    coaches = row[1].split(",")    
                    for coach in coaches:
                        row[1] = coach
                        writer.writerow(row)
                else:
                    writer.writerow(row)


if __name__ == "__main__":
    main()
