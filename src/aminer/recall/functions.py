import numpy as np

from scipy import integrate
from scipy.special import gamma


def chi_square(contingency_table):
    """
    :param contingency_table: 2x2 list of floats

    :return:
        chi square value defined in part III A of
        https://ieeexplore.ieee.org/document/7279056?fbclid=IwAR2YbsiF_aWB94AX_h413rlAfYqGHuBmEbusuYXSW4m1kW-eNIhxHMf1wFs
    """
    n11 = contingency_table[0][0]
    n12 = contingency_table[0][1]
    n21 = contingency_table[1][0]
    n22 = contingency_table[1][1]

    n = n11 + n12 + n21 + n22

    a = (abs(n22 * n11 - n12 * n21) - n/2) / ((n11 + n12) * (n21 + n22))
    b = (abs(n22 * n11 - n12 * n21) - n/2) / ((n11 + n21) * (n12 + n22))

    return a * b


def prob(x):
    """
    :param x: float
        chi square value calculated from chi_square
    :return:
        prob value defined in part III A of
        https://ieeexplore.ieee.org/document/7279056?fbclid=IwAR2YbsiF_aWB94AX_h413rlAfYqGHuBmEbusuYXSW4m1kW-eNIhxHMf1wFs
    """
    return integrate.quad(lambda t: t**(-0.5) * np.exp(-t/2), 0, x) / (np.sqrt(2)*gamma(0.5))


if __name__ == '__main__':
    print(chi_square([[1, 1],
                      [1, 1]]))

    print(prob(0.25)[0])
