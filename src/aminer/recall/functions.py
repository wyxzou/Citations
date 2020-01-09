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

    a = (abs(n22 * n11 - n12 * n21) - n / 2)

    b = ((n11 + n12) * (n21 + n22)) * ((n11 + n21) * (n12 + n22))

    if b == 0:
        return 0
    else:
        return a * a / b


def prob(x):
    """
    :param x: float
        chi square value calculated from chi_square
    :return:
        prob value defined in part III A of
        https://ieeexplore.ieee.org/document/7279056?fbclid=IwAR2YbsiF_aWB94AX_h413rlAfYqGHuBmEbusuYXSW4m1kW-eNIhxHMf1wFs
    """
    return integrate.quad(lambda t: t ** (-0.5) * np.exp(-t / 2), 0, x) / (np.sqrt(2) * gamma(0.5))


def cosine_similarity(v1, v2):
    """
    :param v1: list of integers
        A list of integers representing a paper vector
    :param v2: list of integers
        A list of integers representing a paper vector
    :return:
        cosine similarity as defined in part III B of
        https://ieeexplore.ieee.org/document/7279056?fbclid=IwAR2YbsiF_aWB94AX_h413rlAfYqGHuBmEbusuYXSW4m1kW-eNIhxHMf1wFs
    """
    v1_array = np.array(v1)
    v2_array = np.array(v2)
    dot = np.dot(v1_array, v2_array)
    norm_v1 = np.linalg.norm(v1_array)
    norm_v2 = np.linalg.norm(v2_array)
    return dot / (norm_v1 * norm_v2)


if __name__ == '__main__':
    print(chi_square([[1, 1],
                      [1, 1]]))

    print(prob(0.25)[0])
    print(cosine_similarity([1, 0, 0], [1, 1, 1]))
