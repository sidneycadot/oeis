#! /usr/bin/env python3

import sys
from collections import OrderedDict, Counter
import pickle

class Sequence:
    def __init__(self, first_index, last_index):
        self._first_index = first_index
        self._last_index  = last_index

    @property
    def first_index(self):
        # The first index for which the sequence is defined.
        return self._first_index

    @property
    def last_index(self):
        # The last index for which the sequence is defined.
        # For infinite sequences, this is None.
        return self._last_index

class SkipSequence:
    """ A sequence that we (temporarily) omit, because it turns up as a false positive.
        for some family of sequences and interferes with the catalog-building work.
    """
    def __init__(self, reason):
        self.reason = reason

class PolynomialSequence:

    def __init__(self, coefficients):
        self.coefficients = coefficients

    def __getitem__(self, index):
        r = 0
        q = 1
        for c in self.coefficients:
            r += q * c
            q *= index
        return r

    @property
    def offset(self):
        return (0, 2)

class PowerSequence:
    def __init__(self, multiplier, base, constant_offset):
        assert multiplier >= 1
        assert base > 1
        self.multiplier      = multiplier
        self.base            = base
        self.constant_offset = constant_offset

    def __getitem__(self, index):
        return self.multiplier * self.base ** index + self.constant_offset

    @property
    def offset(self):
        return (0, 2)

class FiniteSequence:
    def __init__(self, values):
        self.values = values

    def __getitem__(self, index):
        assert 0 <= index < len(self.values)
        return self.values[index]

    @property
    def offset(self):
        return (0, 2)

catalog = OrderedDict([

    (     4 , PolynomialSequence([])),
    (    12 , PolynomialSequence([1])),
    (    27 , PolynomialSequence([1, 1])),

    (    79 , PowerSequence(   1,       2, 0)),
    (   244 , PowerSequence(   1,       3, 0)),
    (   290 , PolynomialSequence([0, 0, 1])),
    (   302 , PowerSequence(   1,       4, 0)),
    (   351 , PowerSequence(   1,       5, 0)),
    (   400 , PowerSequence(   1,       6, 0)),
    (   420 , PowerSequence(   1,       7, 0)),
    (   466 , PolynomialSequence([-1, 0, 4])),
    (   578 , PolynomialSequence([0, 0, 0, 1])),
    (  1018 , PowerSequence(   1,       8, 0)),
    (  1019 , PowerSequence(   1,       9, 0)),
    (  1020 , PowerSequence(   1,      11, 0)),
    (  1021 , PowerSequence(   1,      12, 0)),
    (  1022 , PowerSequence(   1,      13, 0)),
    (  1023 , PowerSequence(   1,      14, 0)),
    (  1024 , PowerSequence(   1,      15, 0)),
    (  1025 , PowerSequence(   1,      16, 0)),
    (  1026 , PowerSequence(   1,      17, 0)),
    (  1027 , PowerSequence(   1,      18, 0)),

    (  1029 , PowerSequence(   1,      19, 0)),

    (  1477 , PolynomialSequence([0,  1])),
    (  1478 , PolynomialSequence([-1,  -1])),
    (  1489 , PolynomialSequence([0, -1])),

    (  2066 , PowerSequence(  10,       4, 0)),

    (  4766 , PolynomialSequence([5, 4])),
    (  4767 , PolynomialSequence([3, 4])),
    (  4768 , PolynomialSequence([9, 8])),
    (  4769 , PolynomialSequence([11, 8])),
    (  4770 , PolynomialSequence([5, 8])),
    (  4771 , PolynomialSequence([7, 8])),

    (  4924,  SkipSequence("unknown")),
    (  4926,  SkipSequence("unknown")),
    (  4928,  SkipSequence("unknown")),
    (  4930,  SkipSequence("unknown")),
    (  4932,  SkipSequence("unknown")),
    (  4934,  SkipSequence("unknown")),

    (  4944,  SkipSequence("unknown")),
    (  4945,  SkipSequence("unknown")),
    (  4946,  SkipSequence("unknown")),
    (  4947,  SkipSequence("unknown")),
    (  4948,  SkipSequence("unknown")),
    (  4949,  SkipSequence("unknown")),
    (  4950,  SkipSequence("unknown")),
    (  4951,  SkipSequence("unknown")),
    (  4952,  SkipSequence("unknown")),
    (  4953,  SkipSequence("unknown")),
    (  4954,  SkipSequence("unknown")),
    (  4955,  SkipSequence("unknown")),

    (  4963,  SkipSequence("unknown")),
    (  4965,  SkipSequence("unknown")),
    (  4967,  SkipSequence("unknown")),
    (  4969,  SkipSequence("unknown")),
    (  4971,  SkipSequence("unknown")),
    (  4973,  SkipSequence("unknown")),
    (  4975,  SkipSequence("unknown")),

    (  5010 , PowerSequence(   9,       2, 0)),
    (  5015 , PowerSequence(  11,       2, 0)),
    (  5029 , PowerSequence(  13,       2, 0)),
    (  5030 , PowerSequence(   5,       3, 0)),
    (  5032 , PowerSequence(   7,       3, 0)),
    (  5051 , PowerSequence(   8,       3, 0)),
    (  5052 , PowerSequence(  10,       3, 0)),
    (  5055 , PowerSequence(   7,       5, 0)),

    (  5408 , PolynomialSequence([1, 2])),

    (  5843 , PolynomialSequence([0, 2])),

    (  7395 , PolynomialSequence([2])),

    (  7531 , PolynomialSequence([0, 2, -3, 1])),

    (  8553,  SkipSequence("unknown")),

    (  8585 , PolynomialSequence([0,  3])),
    (  8586 , PolynomialSequence([0,  4])),
    (  8587 , PolynomialSequence([0,  5])),
    (  8588 , PolynomialSequence([0,  6])),
    (  8589 , PolynomialSequence([0,  7])),
    (  8590 , PolynomialSequence([0,  8])),
    (  8591 , PolynomialSequence([0,  9])),
    (  8592 , PolynomialSequence([0, 10])),
    (  8593 , PolynomialSequence([0, 11])),
    (  8594 , PolynomialSequence([0, 12])),
    (  8595 , PolynomialSequence([0, 13])),
    (  8596 , PolynomialSequence([0, 14])),
    (  8597 , PolynomialSequence([0, 15])),
    (  8598 , PolynomialSequence([0, 16])),
    (  8599 , PolynomialSequence([0, 17])),
    (  8600 , PolynomialSequence([0, 18])),
    (  8601 , PolynomialSequence([0, 19])),
    (  8602 , PolynomialSequence([0, 20])),
    (  8603 , PolynomialSequence([0, 21])),
    (  8604 , PolynomialSequence([0, 22])),
    (  8605 , PolynomialSequence([0, 23])),
    (  8606 , PolynomialSequence([0, 24])),
    (  8607 , PolynomialSequence([0, 25])),

    (  9056 , PolynomialSequence([3, 1])),

    (  9964 , PowerSequence(   1,      20, 0)),
    (  9965 , PowerSequence(   1,      21, 0)),
    (  9966 , PowerSequence(   1,      22, 0)),
    (  9967 , PowerSequence(   1,      23, 0)),
    (  9968 , PowerSequence(   1,      24, 0)),
    (  9969 , PowerSequence(   1,      25, 0)),
    (  9970 , PowerSequence(   1,      26, 0)),
    (  9971 , PowerSequence(   1,      27, 0)),
    (  9972 , PowerSequence(   1,      28, 0)),
    (  9973 , PowerSequence(   1,      29, 0)),
    (  9974 , PowerSequence(   1,      30, 0)),
    (  9975 , PowerSequence(   1,      31, 0)),
    (  9976 , PowerSequence(   1,      32, 0)),
    (  9977 , PowerSequence(   1,      33, 0)),
    (  9978 , PowerSequence(   1,      34, 0)),
    (  9979 , PowerSequence(   1,      35, 0)),
    (  9980 , PowerSequence(   1,      36, 0)),
    (  9981 , PowerSequence(   1,      37, 0)),
    (  9982 , PowerSequence(   1,      38, 0)),
    (  9983 , PowerSequence(   1,      39, 0)),
    (  9984 , PowerSequence(   1,      40, 0)),
    (  9985 , PowerSequence(   1,      41, 0)),
    (  9986 , PowerSequence(   1,      42, 0)),
    (  9987 , PowerSequence(   1,      43, 0)),
    (  9988 , PowerSequence(   1,      44, 0)),
    (  9989 , PowerSequence(   1,      45, 0)),
    (  9990 , PowerSequence(   1,      46, 0)),
    (  9991 , PowerSequence(   1,      47, 0)),
    (  9992 , PowerSequence(   1,      48, 0)),

    ( 10037 , SkipSequence("unknown")), # may be polynomial? investigate.

    ( 10692 , PolynomialSequence([10])),
    ( 10701 , PolynomialSequence([3])),
    ( 10709 , PolynomialSequence([4])),
    ( 10716 , PolynomialSequence([5])),
    ( 10722 , PolynomialSequence([6])),
    ( 10727 , PolynomialSequence([7])),
    ( 10731 , PolynomialSequence([8])),
    ( 10734 , PolynomialSequence([9])),

    ( 10850 , PolynomialSequence([11])),
    ( 10851 , PolynomialSequence([12])),
    ( 10852 , PolynomialSequence([13])),
    ( 10853 , PolynomialSequence([14])),
    ( 10854 , PolynomialSequence([15])),
    ( 10855 , PolynomialSequence([16])),
    ( 10856 , PolynomialSequence([17])),
    ( 10857 , PolynomialSequence([18])),
    ( 10858 , PolynomialSequence([19])),
    ( 10859 , PolynomialSequence([20])),
    ( 10860 , PolynomialSequence([21])),
    ( 10861 , PolynomialSequence([22])),
    ( 10862 , PolynomialSequence([23])),
    ( 10863 , PolynomialSequence([24])),
    ( 10864 , PolynomialSequence([25])),
    ( 10865 , PolynomialSequence([26])),
    ( 10866 , PolynomialSequence([27])),
    ( 10867 , PolynomialSequence([28])),
    ( 10868 , PolynomialSequence([29])),
    ( 10869 , PolynomialSequence([30])),
    ( 10870 , PolynomialSequence([31])),
    ( 10871 , PolynomialSequence([32])),

    ( 11379 , PolynomialSequence([0, 0, 1, 1])),

    ( 13748 , PowerSequence(  11,    1331, 0)),
    ( 13749 , PowerSequence( 121,    1331, 0)),
    ( 13750 , PowerSequence(  12,    1728, 0)),
    ( 13833 , PowerSequence( 256,    1024, 0)),
    ( 13837 , PowerSequence( 625,    3125, 0)),
    ( 13840 , PowerSequence( 216,    7776, 0)),
    ( 13841 , PowerSequence(1296,    7776, 0)),
    ( 13842 , PowerSequence(   7,   16807, 0)),

    ( 15245 , PolynomialSequence([0, 0, -11, 2])),

    ( 16777 , PolynomialSequence([1, 3])),
    ( 16789 , PolynomialSequence([2, 3])),

    ( 16813 , PolynomialSequence([1, 4])),
    ( 16825 , PolynomialSequence([2, 4])),

    ( 16861 , PolynomialSequence([1, 5])),
    ( 16873 , PolynomialSequence([2, 5])),
    ( 16885 , PolynomialSequence([3, 5])),
    ( 16897 , PolynomialSequence([4, 5])),

    ( 16921 , PolynomialSequence([1, 6])),
    ( 16933 , PolynomialSequence([2, 6])),
    ( 16945 , PolynomialSequence([3, 6])),
    ( 16957 , PolynomialSequence([4, 6])),
    ( 16969 , PolynomialSequence([5, 6])),

    ( 16993 , PolynomialSequence([1, 7])),
    ( 17005 , PolynomialSequence([2, 7])),
    ( 17017 , PolynomialSequence([3, 7])),
    ( 17029 , PolynomialSequence([4, 7])),
    ( 17041 , PolynomialSequence([5, 7])),
    ( 17053 , PolynomialSequence([6, 7])),

    ( 17077 , PolynomialSequence([1, 8])),
    ( 17089 , PolynomialSequence([2, 8])),
    ( 17101 , PolynomialSequence([3, 8])),
    ( 17113 , PolynomialSequence([4, 8])),
    ( 17137 , PolynomialSequence([6, 8])),
    ( 17149 , PolynomialSequence([7, 8])),
    ( 17173 , PolynomialSequence([1, 9])),
    ( 17185 , PolynomialSequence([2, 9])),
    ( 17197 , PolynomialSequence([3, 9])),
    ( 17209 , PolynomialSequence([4, 9])),
    ( 17221 , PolynomialSequence([5, 9])),
    ( 17233 , PolynomialSequence([6, 9])),
    ( 17245 , PolynomialSequence([7, 9])),
    ( 17257 , PolynomialSequence([8, 9])),

    ( 17281 , PolynomialSequence([1, 10])),
    ( 17293 , PolynomialSequence([2, 10])),
    ( 17305 , PolynomialSequence([3, 10])),
    ( 17317 , PolynomialSequence([4, 10])),
    ( 17329 , PolynomialSequence([5, 10])),
    ( 17341 , PolynomialSequence([6, 10])),
    ( 17353 , PolynomialSequence([7, 10])),
    ( 17365 , PolynomialSequence([8, 10])),
    ( 17377 , PolynomialSequence([9, 10])),

    ( 17401 , PolynomialSequence([1, 11])),
    ( 17413 , PolynomialSequence([2, 11])),
    ( 17425 , PolynomialSequence([3, 11])),
    ( 17437 , PolynomialSequence([4, 11])),
    ( 17449 , PolynomialSequence([5, 11])),
    ( 17461 , PolynomialSequence([6, 11])),
    ( 17473 , PolynomialSequence([7, 11])),
    ( 17485 , PolynomialSequence([8, 11])),
    ( 17497 , PolynomialSequence([9, 11])),
    ( 17509 , PolynomialSequence([10, 11])),

    ( 17533 , PolynomialSequence([1, 12])),
    ( 17545 , PolynomialSequence([2, 12])),
    ( 17557 , PolynomialSequence([3, 12])),
    ( 17569 , PolynomialSequence([4, 12])),
    ( 17581 , PolynomialSequence([5, 12])),
    ( 17593 , PolynomialSequence([6, 12])),
    ( 17605 , PolynomialSequence([7, 12])),
    ( 17617 , PolynomialSequence([8, 12])),
    ( 17629 , PolynomialSequence([9, 12])),
    ( 17641 , PolynomialSequence([10, 12])),
    ( 17653 , PolynomialSequence([11, 12])),

    ( 17654 , PolynomialSequence([121, 264, 144])),
    ( 17655 , PolynomialSequence([1331, 4356, 4752, 1728])),

    ( 20705 , PolynomialSequence([4, 1])),

    ( 20707 , PowerSequence(   4,       2, 0)),

    ( 20710 , PolynomialSequence([5, 1])),
    ( 20714 , PowerSequence(   5,       2, 0)),
    ( 20715 , PolynomialSequence([6, 1])),
    ( 20719 , PolynomialSequence([7, 1])),
    ( 20722 , PolynomialSequence([8, 1])),
    ( 20723 , PolynomialSequence([9, 1])),
    ( 20725 , PolynomialSequence([2, 1])),
    ( 20729 , PowerSequence(   2,       5, 0)),
    ( 20735 , PolynomialSequence([5, 2])),
    ( 20739 , PolynomialSequence([6, 2])),

    ( 20742 , SkipSequence("looks linear, Pisot")),
    ( 20744 , SkipSequence("looks linear, Pisot")),

    ( 22958 , PolynomialSequence([2, -1])),
    ( 22959 , PolynomialSequence([3, -1])),
    ( 22960 , PolynomialSequence([4, -1])),
    ( 22961 , PolynomialSequence([5, -1])),
    ( 22962 , PolynomialSequence([6, -1])),
    ( 22963 , PolynomialSequence([7, -1])),
    ( 22964 , PolynomialSequence([8, -1])),
    ( 22965 , PolynomialSequence([9, -1])),
    ( 22966 , PolynomialSequence([10, -1])),
    ( 22967 , PolynomialSequence([11, -1])),
    ( 22968 , PolynomialSequence([12, -1])),
    ( 22969 , PolynomialSequence([13, -1])),
    ( 22970 , PolynomialSequence([14, -1])),
    ( 22971 , PolynomialSequence([15, -1])),
    ( 22972 , PolynomialSequence([16, -1])),
    ( 22973 , PolynomialSequence([17, -1])),
    ( 22974 , PolynomialSequence([18, -1])),
    ( 22975 , PolynomialSequence([19, -1])),
    ( 22976 , PolynomialSequence([20, -1])),
    ( 22977 , PolynomialSequence([21, -1])),
    ( 22978 , PolynomialSequence([22, -1])),
    ( 22979 , PolynomialSequence([23, -1])),
    ( 22980 , PolynomialSequence([24, -1])),
    ( 22981 , PolynomialSequence([25, -1])),
    ( 22982 , PolynomialSequence([26, -1])),
    ( 22983 , PolynomialSequence([27, -1])),
    ( 22984 , PolynomialSequence([28, -1])),
    ( 22985 , PolynomialSequence([29, -1])),
    ( 22986 , PolynomialSequence([30, -1])),
    ( 22987 , PolynomialSequence([31, -1])),
    ( 22988 , PolynomialSequence([32, -1])),
    ( 22989 , PolynomialSequence([33, -1])),
    ( 22990 , PolynomialSequence([34, -1])),
    ( 22991 , PolynomialSequence([35, -1])),
    ( 22992 , PolynomialSequence([36, -1])),
    ( 22993 , PolynomialSequence([37, -1])),
    ( 22994 , PolynomialSequence([38, -1])),
    ( 22995 , PolynomialSequence([39, -1])),
    ( 22996 , PolynomialSequence([40, -1])),

    ( 23443 , PolynomialSequence([-1, 1])),
    ( 23444 , PolynomialSequence([-2, 1])),
    ( 23445 , PolynomialSequence([-3, 1])),
    ( 23446 , PolynomialSequence([-4, 1])),
    ( 23447 , PolynomialSequence([-5, 1])),
    ( 23448 , PolynomialSequence([-6, 1])),
    ( 23449 , PolynomialSequence([-7, 1])),
    ( 23450 , PolynomialSequence([-8, 1])),
    ( 23451 , PolynomialSequence([-9, 1])),
    ( 23452 , PolynomialSequence([-10, 1])),
    ( 23453 , PolynomialSequence([-11, 1])),
    ( 23454 , PolynomialSequence([-12, 1])),
    ( 23455 , PolynomialSequence([-13, 1])),
    ( 23456 , PolynomialSequence([-14, 1])),
    ( 23457 , PolynomialSequence([-15, 1])),
    ( 23458 , PolynomialSequence([-16, 1])),
    ( 23459 , PolynomialSequence([-17, 1])),
    ( 23460 , PolynomialSequence([-18, 1])),
    ( 23461 , PolynomialSequence([-19, 1])),
    ( 23462 , PolynomialSequence([-20, 1])),

    ( 23463 , PolynomialSequence([-21, 1])),
    ( 23464 , PolynomialSequence([-22, 1])),
    ( 23465 , PolynomialSequence([-23, 1])),
    ( 23466 , PolynomialSequence([-24, 1])),
    ( 23467 , PolynomialSequence([-25, 1])),
    ( 23468 , PolynomialSequence([-26, 1])),
    ( 23469 , PolynomialSequence([-27, 1])),
    ( 23470 , PolynomialSequence([-28, 1])),
    ( 23471 , PolynomialSequence([-29, 1])),
    ( 23472 , PolynomialSequence([-30, 1])),
    ( 23473 , PolynomialSequence([-31, 1])),
    ( 23474 , PolynomialSequence([-32, 1])),
    ( 23475 , PolynomialSequence([-33, 1])),
    ( 23476 , PolynomialSequence([-34, 1])),
    ( 23477 , PolynomialSequence([-35, 1])),
    ( 23478 , PolynomialSequence([-36, 1])),
    ( 23479 , PolynomialSequence([-37, 1])),
    ( 23480 , PolynomialSequence([-38, 1])),
    ( 23481 , PolynomialSequence([-39, 1])),
    ( 23482 , PolynomialSequence([-40, 1])),

    ( 24000 , PolynomialSequence([1, -1])),

    ( 27444 , PolynomialSequence([0, 1, 1, 1])),

    ( 27688 , PolynomialSequence([3, 1, 1])),
    ( 27689 , PolynomialSequence([4, 1, 1])),
    ( 27690 , PolynomialSequence([5, 1, 1])),
    ( 27691 , PolynomialSequence([6, 1, 1])),
    ( 27692 , PolynomialSequence([7, 1, 1])),
    ( 27693 , PolynomialSequence([8, 1, 1])),
    ( 27694 , PolynomialSequence([9, 1, 1])),

    ( 30100 , SkipSequence("unknown")),
    ( 30700 , SkipSequence("unknown")),

    ( 31193 , SkipSequence("looks linear, examine")),

    ( 31500 , SkipSequence("unknown")),

    ( 32614 , SkipSequence("unknown")),

    ( 33168 , SkipSequence("probably finite")),

    ( 33581 , PolynomialSequence([0, 0, 6])),
    ( 33582 , PolynomialSequence([0, 0, 7])),
    ( 33583 , PolynomialSequence([0, 0, 10])),
    ( 33584 , PolynomialSequence([0, 0, 11])),
    ( 33585 , PolynomialSequence([0, 2, 8])),
    ( 33586 , PolynomialSequence([0, 4, 8])),
    ( 33587 , PolynomialSequence([0, 6, 8])),
    ( 33588 , PolynomialSequence([0, -2, 8])),

    ( 33991 , PolynomialSequence([0, -1, 4])),

    ( 35329 , PolynomialSequence([0, 35, 24, 4])),

    ( 36563 , PowerSequence(   1,       2, -3)),

    ( 38865 , PolynomialSequence([63, 45, 9])),
    ( 38866 , PolynomialSequence([124, 72, 12])),
    ( 38867 , PolynomialSequence([125, 75, 15])),

    ( 44102 , PolynomialSequence([0, 36])),

    ( 44138 , SkipSequence("unknown")),
    ( 44179 , SkipSequence("unknown")),
    ( 44187 , SkipSequence("unknown")),
    ( 44242 , SkipSequence("unknown")),
    ( 44251 , SkipSequence("unknown")),
    ( 44322 , SkipSequence("unknown")),
    ( 44421 , SkipSequence("unknown")),

    ( 48488 , PowerSequence(   6,       2, -5)),
    ( 48489 , PowerSequence(   7,       2, -6)),

    ( 51062 , PolynomialSequence([8, 16])),

    ( 51633 , PowerSequence(   5,       2, -2)),

    ( 52268 , PowerSequence(   9,      10, 0)),

    ( 51062 , PolynomialSequence([8, 16])),

    ( 53654 , PolynomialSequence([123456789, 123456789])),

    ( 53741 , PolynomialSequence([20, 50])),
    ( 53742 , PolynomialSequence([25, 50])),
    ( 53743 , PolynomialSequence([45, 100])),

    ( 59558 , SkipSequence("linear?")),

    ( 63941 , PowerSequence(  17,      39, 0)),

    ( 69984 , PolynomialSequence([1123, 21460])),

    ( 70189 , PolynomialSequence([0, 12345679])),

    ( 73762 , PolynomialSequence([12, 24])),

    ( 76338 , PolynomialSequence([1, 512])),

    ( 78787 , PolynomialSequence([1, 101])),

    ( 82285 , PolynomialSequence([13, 16])),
    ( 82286 , PolynomialSequence([10, 18])),
    ( 82369 , PolynomialSequence([13, 30])),

    ( 85959 , PolynomialSequence([0, 37])),

    ( 86224 , PowerSequence(   7,       2, -1)),
    ( 86225 , PowerSequence(  11,       2, -1)),

    ( 86746 , PolynomialSequence([3018, 3018])),

    ( 87113 , PolynomialSequence([2, 2])),
    ( 87752 , PowerSequence(   1,      49, 0)),

    ( 89143 , PowerSequence(   9,       2, -6)),

    ( 89357 , PowerSequence(   1,      64, 0)),
    ( 89683 , PowerSequence(  81,      81, 0)),
    ( 96582 , SkipSequence("unknown")),
    ( 96884 , PowerSequence(   1,     101, 0)),
    ( 97659 , PowerSequence(   1,    1001, 0)),

    ( 97802 , PolynomialSequence([3, 75])),
    ( 98502 , PolynomialSequence([12, 16])),

    ( 98608 , PowerSequence(   1,     100, 0)),
    ( 98609 , PowerSequence(   1,     100, -1)),

    (100775 , PolynomialSequence([101, 97])),
    (100776 , PolynomialSequence([1009, 997])),

    (101202 , PolynomialSequence([142857, 142857])),
    (101442 , PolynomialSequence([10007, 9973])),

    (102439 , PolynomialSequence([4, 100])),

    (103214 , PolynomialSequence([1, 24])),
    (103303 , SkipSequence("unknown")),

    (104661 , PolynomialSequence([2, 1])),

    (106839 , PolynomialSequence([11, 16])),

    (109808 , PowerSequence(   2,       7, 0)),
    (110286 , PowerSequence(  15,       2, 0)),
    (110287 , PowerSequence(  17,       2, 0)),
    (110288 , PowerSequence(  19,       2, 0)),

    (112414 , PolynomialSequence([7, 3])),

    (116530 , PowerSequence(  20,       3, 0)),

    (118759 , SkipSequence("unknown")),
    (118760 , SkipSequence("unknown")),

    (119413 , PolynomialSequence([4, 16])),

    (120354 , PowerSequence(  11,       3, 0)),

    (121377 , SkipSequence("unknown")),
    (121378 , SkipSequence("unknown")),

    (124388 , PolynomialSequence([18, 27])),

    (126978 , PolynomialSequence([9977, 104])),
    (126979 , PolynomialSequence([233, 24])),
    (126980 , PolynomialSequence([47, 14])),

    (127547 , PolynomialSequence([4, 13])),

    (128468 , PolynomialSequence([17, 30])),

    (128470 , PolynomialSequence([1, 30])),
    (128471 , PolynomialSequence([7, 30])),

    (130764, SkipSequence("unknown")),
    (130765, SkipSequence("unknown")),

    (131877 , PolynomialSequence([1, 14])),

    (133752 , PowerSequence(   1,     256, 0)),

    (134297 , PolynomialSequence([107, 107])),

    (134960 , PolynomialSequence([0, 453060])),

    (135403 , PolynomialSequence([1, 111110])),

    (135628 , PolynomialSequence([0, 28])),
    (135631 , PolynomialSequence([0, 31])),
    (135639 , PolynomialSequence([0, 839])),

    (135640 , PowerSequence(   1,     839, 0)),

    (135659 , PolynomialSequence([7, 24])),

    (136602 , SkipSequence("unknown")),

    (138127 , PolynomialSequence([0, 127])),
    (138128 , PowerSequence(   1,     127, 0)),

    (138129 , PolynomialSequence([0, 1729])),
    (138130 , PowerSequence(   1,    1729, 0)),

    (139222 , PolynomialSequence([3, 30])),

    (139245 , PolynomialSequence([4, 20])),
    (139249 , PolynomialSequence([6, 30])),

    (139264 , PolynomialSequence([7, 70])),
    (139279 , PolynomialSequence([8, 40])),
    (139280 , PolynomialSequence([9, 90])),

    (139606 , PolynomialSequence([6, 15])),
    (139607 , PolynomialSequence([7, 21])),
    (139608 , PolynomialSequence([8, 28])),
    (139609 , PolynomialSequence([9, 36])),
    (139610 , PolynomialSequence([10, 45])),
    (139611 , PolynomialSequence([11, 55])),
    (139612 , PolynomialSequence([12, 66])),
    (139613 , PolynomialSequence([13, 78])),
    (139614 , PolynomialSequence([14, 91])),
    (139615 , PolynomialSequence([15, 105])),
    (139616 , PolynomialSequence([16, 120])),
    (139617 , PolynomialSequence([17, 136])),
    (139618 , PolynomialSequence([18, 153])),
    (139619 , PolynomialSequence([19, 171])),
    (139620 , PolynomialSequence([20, 190])),

    (140300 , PowerSequence(   1,    1024, 0)),

    (140801 , PolynomialSequence([360, 720])),

    (142241 , PolynomialSequence([14, 24])),

    (144396 , PolynomialSequence([3, 2])),

    (144619 , PolynomialSequence([8, 19])),

    (147587 , PolynomialSequence([7, 14])),

    (152691 , PolynomialSequence([0, 64])),

    (153080 , PolynomialSequence([2, 13])),

    (153814 , PolynomialSequence([1001, 1001])),

    (153893 , PowerSequence(   3,    2, -1)),
    (153894 , PowerSequence(   5,    2, -1)),
    (153972 , PowerSequence(   1,    2, +6)),

    (154266 , PolynomialSequence([12, 27])),
    (154267 , PolynomialSequence([15, 27])),

    (154519 , PolynomialSequence([228, 216])),
    (154549 , PolynomialSequence([111111, 111111])),

    (155477 , PowerSequence(  43,    1849, 0)),

    (156795 , PolynomialSequence([22, 81])),

    (157105 , PolynomialSequence([106846, 137842])),

    (158000 , PolynomialSequence([339, 338])),
    (158060 , PolynomialSequence([26, 25])),
    (158071 , PolynomialSequence([65, 64])),

    (158130 , PolynomialSequence([120, 121])),
    (158131 , PolynomialSequence([122, 121])),
    (158133 , PolynomialSequence([145, 144])),
    (158136 , PolynomialSequence([143, 144])),
    (158253 , PolynomialSequence([288, 289])),
    (158255 , PolynomialSequence([290, 289])),
    (158313 , PolynomialSequence([401, 400])),

    (158386 , PolynomialSequence([677, 676])),
    (158393 , PolynomialSequence([675, 676])),
    (158399 , PolynomialSequence([783, 784])),
    (158409 , PolynomialSequence([899, 900])),
    (158412 , PolynomialSequence([960, 961])),
    (158414 , PolynomialSequence([962, 961])),
    (158421 , PolynomialSequence([1023, 1024])),

    (159551 , PolynomialSequence([10, 101])),

    (159991 , PowerSequence(   1,      60, 0)),


    (161705 , PolynomialSequence([1, 18])),
    (161709 , PolynomialSequence([1, 22])),
    (161714 , PolynomialSequence([1, 28])),

    (164284 , PolynomialSequence([8, 15])),

    (164346 , PowerSequence(   3,       4, 0)),

    (165747 , PolynomialSequence([1, -2])),

    (166731 , SkipSequence("english")),

    (168650 , SkipSequence("unknown")),
    (168651 , SkipSequence("unknown")),
    (168652 , SkipSequence("unknown")),

    (169604 , PowerSequence(   3,       6, 0)),
    (169634 , PowerSequence(   3,       7, 0)),
    (169823 , PolynomialSequence([0, 60])),
    (169825 , PolynomialSequence([0, 420])),
    (169827 , PolynomialSequence([0, 840])),

    (170955 , PowerSequence(  10,      10, -9)),

    (172178 , PolynomialSequence([1, 99])),

    (172525 , PolynomialSequence([111111111, 111111111])),

    (174312 , PolynomialSequence([0, 32])),
    (175558 , PowerSequence(   1,     167, 0)),
    (175805 , PowerSequence(  21,       2, 0)),
    (175806 , PowerSequence(  27,       2, 0)),
    (176413 , PowerSequence(  19,       3, 0)),

    (177769 , PolynomialSequence([111, 111])),

    (178027 , PolynomialSequence([0, 5291])),

    (183010 , PolynomialSequence([-1, 24])),

    (186113 , PolynomialSequence([6, 13])),

    (187206 , PolynomialSequence([138, 144])),

    (190991 , PolynomialSequence([1, 13])),

    (193577 , PowerSequence(   5,       7, 0)),
    (195819 , PolynomialSequence([0, 29])),
    (200860 , PolynomialSequence([0, 682])),

    (204189 , SkipSequence("finite")),

    (209876 , PolynomialSequence([30, 36])),

    (213182 , SkipSequence("unknown")),
    (213184 , SkipSequence("unknown")),

    (215137 , PolynomialSequence([1, 17])),
    (215144 , PolynomialSequence([1, 19])),
    (215145 , PolynomialSequence([1, 20])),
    (215146 , PolynomialSequence([1, 21])),
    (215148 , PolynomialSequence([1, 23])),

    (216491 , PowerSequence(  12,       5, 0)),
    (225374 , PowerSequence(   1,     111, 0)),

    (229855 , PolynomialSequence([257, 384])),

    (236432 , PolynomialSequence([210, 420])),

    (238477 , PolynomialSequence([5, 32])),

    (242181 , SkipSequence("unknown")),
    (242215 , PolynomialSequence([5, 18])),

    (242570 , PolynomialSequence([0, 252])),

    (248572 , PolynomialSequence([1, 29])),

    (249674 , PolynomialSequence([0, 30])),

    (250024 , PolynomialSequence([19, 40])),

    (252994 , PolynomialSequence([0, 26])),

    (256958 , PolynomialSequence([-50, 1])),

    (258070 , SkipSequence("unknown")),
    (258071 , SkipSequence("unknown")),
    (258597 , PowerSequence(  13,       3, 0)),
    (258598 , PowerSequence(  17,       3, 0)),
    (259076 , PowerSequence(   1,      80, 0)),

    (260751 , SkipSequence("finite")),

    (261150 , PolynomialSequence([403185216600637, 2124513401010])),
    (261151 , PolynomialSequence([11410337850553, 460909869420])),
    (261152 , PolynomialSequence([161004359399459161, 10644900609172830]))
])

def check_catalog():

    # Verify that the keys in the catalog are increasing:
    bad_order_found = False
    catalog_keys = list(catalog.keys())
    for i in range(len(catalog_keys) - 1):
        if not(catalog_keys[i] < catalog_keys[i + 1]):
            print("bad catalog key order: {}, {}".format(catalog_keys[i], catalog_keys[i + 1]))
            bad_order_found = True

    if bad_order_found:
        print("catalog bad, please fix.")
        sys.exit()

    filename = "oeis.pickle"
    #filename = "oeis-10000.pickle"

    with open(filename, "rb") as f:
        oeis_entries = pickle.load(f)

    if False:
        keyword_counter = Counter()
        for oeis_entry in oeis_entries:
            for keyword in oeis_entry.keywords:
                keyword_counter[keyword] += 1
        print(keyword_counter)
        sys.exit()

    for (oeis_id, catalog_entry) in catalog.items():
        oeis_entry = oeis_entries[oeis_id - 1]
        assert oeis_id == oeis_entry.oeis_id

        if isinstance(catalog_entry, SkipSequence):
            continue

        #print("[A{:06d}] ... checking ...".format(oeis_id))

        #if catalog_entry.offset != oeis_entry.offset:
        #    print("[A{:06d}] OFFSET COMPARISON FAILED: oeis says {}, catalog says {}".format(oeis_id, oeis_entry.offset, catalog_entry.offset))

        catalog_values = [catalog_entry[i] for i in range(len(oeis_entry.values))]

        #print(oeis_entry.values)
        #print(catalog_values)

        if any(catalog_entry[i] != oeis_entry.values[i] for i in range(len(oeis_entry.values))):
            print("[A{:06d}] VALUE COMPARISON FAILED: oeis says {}, catalog says {}".format(oeis_id, oeis_entry.values, catalog_values))

    print("catalog has {} entries.".format(len(catalog)))

def main():
    check_catalog()

if __name__ == "__main__":
    main()
