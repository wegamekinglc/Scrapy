# -*- coding: utf-8 -*-
u"""
Created on 2017-4-19

@author: cheng.li
"""

import sys

if __name__ == '__main__':

    file_name = sys.argv[1]

    with open(file_name, 'r', encoding='gbk') as fh:
        s = fh.readlines()

    with open(file_name, 'w', encoding='utf8') as fh:
        fh.writelines(s)
