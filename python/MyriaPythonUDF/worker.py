from __future__ import print_function
import os
import sys
import time
import socket
import traceback
import struct


from MyriaPythonUDF.serializers import read_int, write_int,SpecialLengths,write_with_length,PickleSerializer

pickleSer = PickleSerializer()


def read_command( file, serializer):
    #read one element, size=1
    command = serializer._read_command(file)
    return command

def main(infile, outfile):
    try:
        #get code
        func = read_command(infile, pickleSer)
        tuplesize = read_int(infile)

        if tuplesize < 1:
            raise ValueError("size of tuple should not be less than 1 ")


        while True:
            tup =pickleSer.read_with_length(infile,tuplesize)
            pickleSer.write_with_length(func(tup),outfile)
            outfile.flush()


    except Exception:
        try:
            write_int(SpecialLengths.PYTHON_EXCEPTION_THROWN,outfile)
            write_with_length(traceback.format_exc().encode("utf-8"),outfile)
            print(traceback.format_exc(), file=sys.stderr)
        except Exception:
            print("python process failed with exception: ", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)

    #if read_int(infile) == SpecialLengths.END_OF_STREAM:
    #    write_int(SpecialLenghts.END_OF_STREAM, outfile)
    #else:
    #    write_int(SpecialLengths.END_OF_DATA_SECTION,outfile)
    #    exit(-1)

if __name__ == '__main__':
    # Read a local port to connect to from stdin
    java_port = int(sys.stdin.readline())
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", java_port))
    infile = os.fdopen(os.dup(sock.fileno()), "rb", 65536)
    outfile = os.fdopen(os.dup(sock.fileno()), "wb", 65536)
    exit_code = 0
    try:
        main(infile,outfile)
    except SystemExit as exc:
        exit_code = exc.code
    finally:
        outfile.flush()
        sock.close()
