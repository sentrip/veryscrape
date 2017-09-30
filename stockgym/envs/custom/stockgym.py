import random
from collections import deque
from multiprocessing.connection import Client

import empyrical as emp
import gym
import numpy as np
import pandas as pd
from gym import spaces
from gym.utils import seeding
from scipy.interpolate import CubicSpline as CS


class StockGym(gym.Env):
    metadata = {'render.modes': ['human']}

    def __init__(self):
        self.id = str(random.randint(10000000, 90000000))
        self.initial_balance = 100000.
        self.bankrupt_threshold = 0.5
        self.updates_every = 60
        self.history_length = 5000
        self.N = 50
        # Buy/sell ratios
        self.ratios = [-1., 0., 1.]
        # (n_owned, price, red_sent, twit_sent, art_sent, blog_sent, long_SMA, lse_derivative, spline_est)
        self.n_company_metrics = 9
        # (balance, agent_value, market_value, max_drawdown, alpha, beta, calmar, sharpe, sortino)
        self.n_portfolio_metrics = 9

        self.incoming_address = ('localhost', 6200)
        self.incoming = Client(self.incoming_address, authkey=b'veryscrape')
        data_dict = self.incoming.recv()
        # State shape for data feeding later
        self.state_shape = (len(data_dict) + 1) * self.n_company_metrics
        assert self.state_shape == self.n_portfolio_metrics + self.n_company_metrics * len(data_dict), \
            "State is not rectangular (change number of company/portfolio metrics)"
        self.balance = self.initial_balance
        self.returns = deque(maxlen=self.history_length)
        self.benchmark_returns = deque(maxlen=self.history_length)
        self.data = {
        c: {'index': i, 'initial_price': 0.01, 'stocks_owned': 0, 'reddit': 0.5, 'twitter': 0.5, 'blog': 0.5,
            'article': 0.5, 'history': deque(maxlen=self.history_length)} for i, c in enumerate(data_dict)}
        self.company_index = {i: c for i, c in enumerate(data_dict)}
        self.ann_factor = int(48 * 5 * 3600 / self.updates_every)
        for c in data_dict:
            for e in data_dict[c]:
                if e == 'stock':
                    self.data[c]['initial_price'] = data_dict[c][e]
                    self.data[c]['history'].append(data_dict[c][e])
                else:
                    self.data[c][e] = data_dict[c][e]

        self.state = np.array([0.] * (self.n_portfolio_metrics + self.n_company_metrics * len(self.company_index)))
        self.action_space = spaces.Discrete(len(self.data) * len(self.ratios))
        self.observation_space = spaces.Box(np.array([-np.finfo(np.float32).max] * len(self.state), dtype=np.float32),
                                            np.array([np.finfo(np.float32).max] * len(self.state), dtype=np.float32))
        self._seed()

    def _reset(self):
        self.balance = self.initial_balance
        self.returns = deque(maxlen=self.history_length)
        self.benchmark_returns = deque(maxlen=self.history_length)
        self.returns.append(0.000001)
        self.benchmark_returns.append(0.000001)
        for c in self.data:
            self.data[c]['initial_price'] = self.data[c]['history'][-1]
            self.data[c]['stocks_owned'] = 0

        while len(self.returns) < 4:
            self.update_lists()
        self.state = self.build_state()
        return self.state

    def _step(self, action):
        # Act
        company = self.company_index[int(action / len(self.ratios))]
        self.submit_order(company, self.ratios[action % len(self.ratios)])
        # Observe
        self.update_lists()
        self.state = self.build_state()
        reward = self.agent_return * self.agent_market_difference_ratio
        done = self.agent_is_broke
        return self.state, reward, done, {}

    def _render(self, mode='human', close=False):
        pass

    def _seed(self, seed=None):
        self.np_random, seed = seeding.np_random(seed)
        return [seed]

    def submit_order(self, company, order):
        """Submits a buy or send order, changing balance and stock number for requested company"""
        # Commission - 0.1% of trade volume
        tc = 0.001
        current_price = self.data[company]['history'][-1]
        if order > 0:
            t = int(self.balance * order / current_price)
            n = int((self.balance - current_price * t * tc) * order / current_price)
        else:
            n = int(self.data[company]['stocks_owned'] * order)
        cost = current_price * n
        self.balance -= (cost + tc * abs(cost))
        self.data[company]['stocks_owned'] += n

    def update_lists(self):
        """Updates current stock price/sentiment/returns lists"""
        data_dict = self.incoming.recv()
        for c in data_dict:
            for e in data_dict[c]:
                if e == 'stock':
                    self.data[c]['history'].append(data_dict[c][e])
                else:
                    self.data[c][e] = data_dict[c][e]
        self.returns.append(self.agent_return)
        self.benchmark_returns.append(self.benchmark_return)

    def build_state(self):
        """
        Builds state up from current financial metrics
        First few values are portfolio metrics, rest are company metrics
        """
        # (balance, agent_value, market_value, max_drawdown, downside_risk, alpha, beta, calmar, sharpe, sortino)
        portf_metrics = [self.balance, self.agent_value, self.market_value, self.max_drawdown,
                         self.alpha_value, self.beta_value, self.calmar_ratio, self.sharpe_ratio, self.sortino_value]
        # (n_owned, price, red_sent, twit_sent, short_SMA, med_SMA, long_SMA, lse_derivative, spline_est)
        c_metrics = [0.] * len(self.data) * self.n_company_metrics
        for c in self.data:
            c_metrics[self.data[c]['index'] * self.n_company_metrics + 0] = self.data[c]['stocks_owned']
            c_metrics[self.data[c]['index'] * self.n_company_metrics + 1] = self.data[c]['history'][-1]
            c_metrics[self.data[c]['index'] * self.n_company_metrics + 2] = self.data[c]['reddit']
            c_metrics[self.data[c]['index'] * self.n_company_metrics + 3] = self.data[c]['twitter']
            c_metrics[self.data[c]['index'] * self.n_company_metrics + 4] = self.data[c]['article']
            c_metrics[self.data[c]['index'] * self.n_company_metrics + 5] = self.data[c]['blog']
            c_metrics[self.data[c]['index'] * self.n_company_metrics + 6] = self.moving_average(c, 3)
            c_metrics[self.data[c]['index'] * self.n_company_metrics + 7] = self.least_square_derivative(c)
            c_metrics[self.data[c]['index'] * self.n_company_metrics + 8] = self.cubic_spline_estimate(c).min()
        return np.array([*portf_metrics, *c_metrics]).reshape([len(self.data) + 1, self.n_company_metrics])

    @property
    def agent_is_broke(self):
        """Current value of all investments and money are less than a threshold percentage of the initial balance"""
        return self.agent_value < self.initial_balance * self.bankrupt_threshold

    @property
    def agent_value(self):
        """Current value of all investments and money"""
        return self.balance + sum(self.data[c]['stocks_owned'] * self.data[c]['history'][-1] * 0.999 for c in self.data)

    @property
    def market_value(self):
        """Average of inital_price/price ratios for all companies multiplied by initial investment"""
        return self.initial_balance * sum(max(0.0001, self.data[c]['history'][-1] / self.data[c]['initial_price'])
                                          for c in self.data) / len(self.data)

    @property
    def agent_market_difference_ratio(self):
        """Scaling factor for agent's performence in comparison to the market"""
        return 1 + (self.agent_value - self.market_value) / self.initial_balance

    @property
    def agent_return(self):
        """Current returns of agent from entire portfolio as excess percentage"""
        return self.agent_value / self.initial_balance - 1

    @property
    def benchmark_return(self):
        """Current returns of market as excess percentage"""
        return self.market_value / self.initial_balance - 1

    @property
    def max_drawdown(self):
        """Current maximum drawdown of portfolio"""
        return emp.max_drawdown(np.array(self.returns))

    @property
    def alpha_value(self):
        """Current alpha value of portfolio"""
        return emp.alpha(pd.Series(self.returns), pd.Series(self.benchmark_returns))

    @property
    def beta_value(self):
        """Current beta value (volatility) of portfolio"""
        return emp.beta(pd.Series(self.returns), pd.Series(self.benchmark_returns))

    @property
    def sharpe_ratio(self):
        """Current risk-adjusted return value of portfolio"""
        r = emp.sharpe_ratio(pd.Series(self.returns), annualization=self.ann_factor)
        return r if r is not np.nan else 0.

    @property
    def calmar_ratio(self):
        """Current calmar value of portfolio"""
        r = emp.calmar_ratio(pd.Series(self.returns), annualization=self.ann_factor)
        return r if r is not np.nan else 0.

    @property
    def downside_risk(self):
        """Current downside risk of porfolio"""
        return emp.downside_risk(pd.Series(self.returns), annualization=self.ann_factor)

    @property
    def sortino_value(self):
        """Current sortino value of portfolio"""
        if self.downside_risk == 0.:
            r = 0.
        else:
            r = emp.sortino_ratio(pd.Series(self.returns), annualization=self.ann_factor,
                                  _downside_risk=self.downside_risk)
        return r if r is not np.nan else 0.

    def moving_average(self, company, factor):
        """Simple moving average for previous factor*N stock prices"""
        return sum(
            self.data[company]['history'][i] for i in
            range(-min(factor * self.N, len(self.data[company]['history'])), 0)) / min(
            factor * self.N, len(self.data[company]['history']))

    def least_square_derivative(self, company):
        """Derivative of linear least-squares estimates for previous N stock prices"""
        n, s_x, s_y, s_xy, s_x2, s_y2 = 0, 0, 0, 0, 0, 0
        for i, j in enumerate(reversed(self.data[company]['history'])):
            if n >= self.N:
                break
            n += 1
            s_x += i
            s_y += j
            s_xy += i * j
            s_x2 += i ** 2
            s_y2 += j ** 2
        return (n * s_xy - s_x * s_y) / (n * s_x2 - s_x ** 2)

    def cubic_spline_estimate(self, company):
        """Estimated stock price based on cubic splines fit on previous N stock prices"""
        n_prices = min(len(self.data[company]['history']), int(self.N / 5))
        s = CS(list(range(n_prices)), list(self.data[company]['history'])[-int(self.N / 5):], extrapolate=True)
        return s(n_prices + 1)
