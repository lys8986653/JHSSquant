class Optimizer:
    def __init__(self, data, sigmas, ZZ500_weight, second_moment):
        """
        data:含有expected_return项；SecuCode项，为股票代码，类型为int；TradingDay项，类型为pd.datetime。Factor项，用于追踪组合的因子值。
        sigmas:一个协方差矩阵，行名和列名为股票代码的升序排列,np.array,或pd.DataFrame。
        ZZ500_weight:ZZ500行业权重
        second_moment:二阶矩,追踪误差时使用。
        
        追踪结果：
        tosell_weight_1：是在股票池初选的时候扔出的权重。
        tosell_weight_2：是MVO选股时的时候扔出的权重。
        tosell_number_1：是在股票池初选的时候扔出的数量。
        tosell_number_2：是MVO选股时的时候扔出的数量。
        indus_count：记录每天与行业偏离的列表。
        indus_count_bentch:记录bentch与行业偏离的列表。
        factor:记录每天组合的因子值。
        factor_bentch:记录每天组合的因子值。
        inner_turnover:记录每日的内部调仓带来的换手。
        opts：有解的天数。
        nonopts：无解的天数。
        """
        self.data = data
        self.sigmas = sigmas
        self.ZZ500_weight = ZZ500_weight
        self.second_moment = second_moment
        #换手的追踪
        self.tosell_weight_1 = []
        self.tosell_weight_2 = []
        self.tosell_number_1 = []
        self.tosell_number_2 = []
        self.inner_turnover = []
        self.turnover = []
        #有无最优解的追踪
        self.opts = 0
        self.nonopts = 0
        #行业偏离的追踪
        self.indus_count = []
        self.indus_count_bench = []
        #因子大小的追踪
        self.factor = []
        self.factor_bench = []
        #size的追踪
        self.size = []
        self.size_bentch = []
        self.SizeTNoInd = []
        self.SizeTNoInd_bentch = []
        #权重的追踪
        self.indus_number = []
        #相关系数的追踪
        self.cors = []
        self.Return_H =[]
        self.factor_300=[]
        self.factor_100=[]

    def adjust_weights(self, weights, threshold=0.00001):
        """
        目的：优化结束后，下限w可能为很小的负数，比如e-6，调整权重为一合理权重。
        Parameters:
        weights (np.ndarray): 需要调整的权重数组。
        threshold (float): 小于此阈值的权重将被设为0。

        Returns:
        np.ndarray: 调整后的权重数组。
        """
        weights[weights < threshold] = 0
        total_remaining_weight = np.sum(weights)
        if total_remaining_weight == 0:
            return weights
        else:
            return weights / total_remaining_weight * np.sum(weights)

    def select_securities_with_R(self,
                            day,
                            secucodes_use,
                            top_n=10,
                            top_yesterday=90):
        """
        根据self.data中的收益选择符合条件的股票，并更新使用的股票代码。

        Parameters:
        day (pd.Timestamp): 当前处理的日期，今日。
        secucodes_use (list of int): 当前使用的证券代码列表，也就是昨日的股票代码。
        top_n (int): 新增股票池的数量。
        top_yesterday (int): 昨日股票池的数量。

        Returns:
        tuple: 处理后的 secu_use 和更新后的 secucodes_use 的元组。
        默认设置为选取昨日股票中最高的90只与今日剩余的最高的10只作为今日选股。
        """
        # 过滤数据, 当天股票池与昨日100只股票池
        yesterday_use = self.data[(self.data['TradingDay'] == day) &
                                  (self.data['SecuCode'].isin(secucodes_use))]
        yesterday_use = yesterday_use.sort_values(by='expected_return',
                                                  ascending=False)
        secucodes_use = list(yesterday_use.head(top_yesterday)["SecuCode"])

        secu_all = self.data[(self.data['TradingDay'] == day)
                             & (~self.data['SecuCode'].isin(secucodes_use))]
        # 按预期收益排序
        secu_all = secu_all.sort_values(by='expected_return', ascending=False)

        # 新增股票
        secu_select = secu_all.head(top_n)

        # 更新 secucodes_use
        secucodes_use = secucodes_use + list(secu_select['SecuCode'])
        secucodes_use = sorted(secucodes_use)

        # 过滤并排序 secu_use
        secu_use = self.data[(self.data['TradingDay'] == day)
                             & (self.data['SecuCode'].isin(secucodes_use))]
        secu_use = secu_use.sort_values(by='SecuCode')

        return secu_use, secucodes_use

    def select_securities_with_Size(self,
                                    day,
                                    secucodes_use,
                                    top_n=10,
                                    top_yesterday=90):
        # 过滤数据, 当天股票池与昨日100只股票池
        yesterday_use = self.data[(self.data['TradingDay'] == day) &
                                  (self.data['SecuCode'].isin(secucodes_use))]
        dev = np.array(
            yesterday_use.groupby("Size_group").size() -
            int(top_yesterday / 10))
        dev = np.where(dev < 0, dev, 0)
        yesterday_use = yesterday_use.groupby(by="Size_group").apply(
            lambda x: x.sort_values(by='expected_return', ascending=False))
        yesterday_use = yesterday_use.reset_index(drop=True)

        secucodes_use = list(
            yesterday_use.groupby(by="Size_group").apply(
                lambda x: x.head(int(top_yesterday / 10))["SecuCode"]))

        secu_all = self.data[(self.data['TradingDay'] == day)
                             & (~self.data['SecuCode'].isin(secucodes_use))]
        # 按预期收益排序
        secu_all = secu_all.groupby(by="Size_group").apply(
            lambda x: x.sort_values(by='expected_return', ascending=False))
        secu_all = secu_all.reset_index(drop=True)
        # 新增股票
        secu_select = secu_all.groupby("Size_group").apply(
            lambda x: x.head(int(top_n / 10) - dev[x.name]))

        # 更新 secucodes_use
        secucodes_use = secucodes_use + list(secu_select['SecuCode'])
        secucodes_use = sorted(secucodes_use)

        # 过滤并排序 secu_use
        secu_use = self.data[(self.data['TradingDay'] == day)
                             & (self.data['SecuCode'].isin(secucodes_use))]
        secu_use = secu_use.sort_values(by='SecuCode')

        return secu_use, secucodes_use

    def select_securities_with_Weight(self,
                            day,
                            w_value,
                            secucodes_use,
                            industry_matrix,
                            secucodes_use_2,
                            tolerance=1e-4):
        """
        根据w_value中的收益选择符合条件的股票，并更新使用的股票代码。

        Parameters:
        day (pd.Timestamp): 当前处理的日期，今日。
        w_value : (np.array):权重，根据该权重去调整。
        secucodes_use (list of int): 当前使用的证券代码列表，也就今日选出的股票代码。
        secucodes_use_2 (list of int): 昨日使用的证券代码列表，当权重相等时，昨天选中的优先，来减少换手。

        Returns:
        tuple: 处理后的 secu_use 和更新后的 secucodes_use 的元组。
        """
        secucodes_number = w_value.size
        # 处理 w_value,标准化
        pri_industry_weight = industry_matrix.T @ w_value
        w_1 = np.where(np.isclose(w_value, 0, atol=3 * 1e-4), 0, 1)
        pri_industry_number = industry_matrix.T @ w_1
        pri_industry_mean = pri_industry_weight / (pri_industry_number + 1e-6)
        pri_industry_mean = np.where(pri_industry_mean > 1, 0,
                                     pri_industry_mean)
        w_value_mean = industry_matrix @ (pri_industry_mean + 1e-6)
        w_value_std = w_value / w_value_mean

        w_value = np.array(w_value_std).reshape(-1)

        # 创建 secucodes_use 的数据框，添加 secucodes_use_2 以减少换手

        secucodes_use_df = pd.Series(index=secucodes_use, data=w_value)
        # 这里给昨天的股票代码 secucodes_use_2 一个微小的权重提升，以便在权重相等时优先选择它们
        secucodes_use_df[secucodes_use_df.index.isin(
            secucodes_use_2)] += tolerance  # 使用一个非常小的值，避免影响排序结果

        # 按照调整后的权重进行排序
        secucodes_use_df = secucodes_use_df.sort_values(ascending=False)
        # 选出前100个股票代码
        secucodes_use_df = secucodes_use_df.head(100)
        secucodes_use = list(secucodes_use_df.index)

        # 对选出的股票代码进行排序
        secucodes_use = sorted(secucodes_use)

        secu_use = self.data[(self.data['TradingDay'] == day)
                             & (self.data['SecuCode'].isin(secucodes_use))]
        secu_use = secu_use.sort_values(by='SecuCode')

        return secu_use, secucodes_use

    def initialize_weights(self, secucodes_use, yesterday):
        """
        Parameters:
        secucodes_use (list of int): 当前使用的证券代码列表，今日股票代码。
        yesterday:昨天。
        
        Returns:
        w_0（np.array n*1）：今日股票代码在昨日的权重向量
        """
        yesterday_all = self.data[self.data['TradingDay'] == yesterday]
        w_0 = pd.Series(yesterday_all['OpenRatio'].values,
                        index=yesterday_all['SecuCode']).reindex(secucodes_use,
                                                                 fill_value=0)
        w_0 = np.array(w_0).reshape(len(secucodes_use), 1)
        return w_0

    def solve_optimization(self,
                           expected_return,
                           sigma,
                           sec,
                           lambda_value,
                           w_0,
                           industry_weight,
                           industry_matrix,
                           secucodes_number,
                           SizeTNoInd=None,
                           SizeTNoInd_lower_bound=False,
                           SizeTNoInd_upper_bound=False,
                           w_value=None,
                           mode=1,
                           weight_lower_bound=0,
                           weight_upper_bound=0.015,
                           industry_deviation=0.1,
                           turnover_bound=0.4,
                           dynamic_weight_bound=False,
                           SizeGroup_matrix = None,
                           SizeGroup_lower_bound=False,
                           SizeGroup_upper_bound=False,
                           priority_list=[1, 1, 1],
                           TE=0.2):
        """
        Parameters:
        expected_return (list of int): 今日收益，并且确保收益是按照股票代码从低到高排得到的。
        sigma:当天的协方差矩阵，确保sigma计算出来时行列名也是按照股票代码从低到高排得到的。
        w_0（np.array n*1）：今日股票代码在昨日的权重向量，同上。
        lambda_value:风险惩罚。
        secucodes_number : 股票代码数量
        w_value:一阶段优化得到的权重
        industry_weight：行业权重。
        industry_matrix:行业矩阵，确保股票代码，行业权重从低到高排。

        mode：优化阶段
        
        Returns:
        problem.status：问题状态。
        w_value : 优化后的权重。
        problem:解决好的问题。
        """
        #
        secucodes_number = expected_return.size
        industry_number = industry_matrix.shape[1]
        #数据初始化，全部转化成np.array与列向量
        expected_return = np.array(expected_return).reshape(-1, 1)
        expected_return = np.nan_to_num(expected_return, nan=-1)
        industry_weight = np.array(industry_weight).reshape(-1, 1)
        industry_matrix = np.array(industry_matrix)
        if not isinstance(SizeGroup_matrix,bool):
            SizeGroup_matrix = np.array(SizeGroup_matrix)
        #根据模式来判断是有费率模型还是无费率模型
        if mode == 1:
            w = cp.Variable(secucodes_number)
            w = w.reshape((secucodes_number, 1))
            if bool(industry_deviation):
                a1 = cp.Variable(industry_number)
                a1 = a1.reshape((industry_number, 1))
                a2 = cp.Variable(industry_number)
                a2 = a2.reshape((industry_number, 1))
            else:
                a1 = 0
                a2 = 0
            if bool(turnover_bound):
                a3 = cp.Variable(1)
            else:
                a3 = 0
            if bool(SizeTNoInd_upper_bound):
                a4 = cp.Variable(1)
            else:
                a4 = 0
            if bool(SizeTNoInd_lower_bound):
                a5 = cp.Variable(1)
            else:
                a5 = 0
            if bool(TE):
                a6 = cp.Variable(1)
            else:
                a6 = 0
            if bool(SizeGroup_lower_bound):
                a8 = cp.Variable(10)
                a8 = a8.reshape((10, 1))
            else:
                a8=np.zeros(10).reshape(-1,1)
            if bool(SizeGroup_upper_bound):
                a7 = cp.Variable(10)
                a7 = a7.reshape((10, 1))
            else:
                a7=np.zeros(10).reshape(-1,1)
            
            #卖出部分权重
            tosell_weight = 1 - sum(w_0)
            w_1 = np.ones(secucodes_number).reshape(-1, 1)
            
            
            if bool(SizeGroup_lower_bound)&bool(SizeGroup_upper_bound):
                SizeGroup_lower_bound = np.zeros(10).reshape(-1,1)
                SizeGroup_upper_bound = np.array([0.1,0.1,0.1,0.1,0.1,0.15,0.15,0.15,0.15,0.15]).reshape(-1,1)
            if True:
                #设置换手的内置最低下限
                if bool(turnover_bound):
                    if turnover_bound <= 2 * tosell_weight:
                        turnover_bound = 2 * tosell_weight
            if bool(industry_deviation):
                #设置行业的最低下限
                d_weight_lower_bound = w_1 * weight_lower_bound
                d_weight_upper_bound = w_1 * weight_upper_bound
                dev1 = np.max(industry_matrix.T @ d_weight_lower_bound -
                              industry_weight)
                dev2 = np.min(industry_matrix.T @ d_weight_upper_bound -
                              industry_weight)
                if dev1 > 0 and dev2 < 0:
                    max_dev = np.max(np.abs([dev1, dev2]))
                    if max_dev > industry_deviation:
                        industry_deviation = np.ceil(max_dev / 0.005) * 0.005

            p1, p2, p3 = self.priority_to_penalty(priority_list)
            #层次换手，无解放开到下一个level
            if True:
                #for indus_limit in np.arange(industry_deviation, 0.2, 0.01):
                #print("indus_limit"+str(indus_limit))
                if True:
                    #for limit in np.arange(turnover_bound, 0.61, 0.05):
                    #利用pd.Series的索引来选择约束
                    indus_limit = industry_deviation
                    limit = turnover_bound
                    #目标
                    objective = cp.Maximize(expected_return.T @ w -
                                            lambda_value *
                                            cp.quad_form(w, sigma) -
                                            p1 * sum(a1 + a2) - p2 * a3 - p3 *
                                            (a4 + a5) - 1000 * a6-100*sum(a7+a8))
                    ind_number = industry_matrix.T @ np.ones(
                        secucodes_number).reshape(-1, 1)
                    indus_limit = np.where(ind_number == 0, 1, indus_limit)
                    constraints = [
                        sum(w) == 1, w >= weight_lower_bound,
                        w <= weight_upper_bound,
                        cp.sum(cp.abs(w - w_0)) <= limit - tosell_weight + a3,
                        industry_matrix.T @ w <=
                        industry_weight + indus_limit + a1,
                        industry_matrix.T @ w >=
                        industry_weight - indus_limit - a2,
                        cp.quad_form(w, sec) <= TE**2 / 252 + a6,
                        SizeTNoInd.T @ w <= SizeTNoInd_upper_bound + a4,
                        SizeTNoInd.T @ w >= SizeTNoInd_lower_bound - a5,
                        SizeGroup_matrix.T@ w <= SizeGroup_upper_bound +a7,
                        SizeGroup_matrix.T@ w >= SizeGroup_lower_bound -a8,
                        a1 >= 0, a2 >= 0, a3 >= 0, a4 >= 0, a5 >= 0, a6 >= 0,a7>=0,a8>=0
                    ]
                    index = [
                        "weight_sum", "weight_lower_bound",
                        "weight_upper_bound", "turnover_bound",
                        "industry_lower_bound", "industry_upper_bound", "TE",
                        "SizeTNoInd_upper_bound", "SizeTNoInd_lower_bound",
                        "Size_upper_bound","Size_lower_bound",
                        "a1", "a2", "a3", "a4", "a5", "a6","a7","a8"
                    ]
                    constraints_all = pd.Series(data=constraints, index=index)
                    constraints_selected = [
                        True, True, True,
                        bool(turnover_bound),
                        bool(industry_deviation),
                        bool(industry_deviation),
                        bool(TE),
                        bool(SizeTNoInd_upper_bound),
                        bool(SizeTNoInd_lower_bound),
                        bool(SizeGroup_lower_bound),
                        bool(SizeGroup_upper_bound),
                        bool(industry_deviation),
                        bool(industry_deviation),
                        bool(turnover_bound),
                        bool(SizeTNoInd_upper_bound),
                        bool(SizeTNoInd_lower_bound),
                        bool(TE),
                        bool(SizeGroup_lower_bound),
                        bool(SizeGroup_upper_bound)
                    ]
                    constraints_selected = list(
                        constraints_all[constraints_selected].values)
                    problem = cp.Problem(objective, constraints_selected)
                    try:
                        problem.solve(solver=cp.MOSEK,
                                      mosek_params={
                                          'MSK_DPAR_OPTIMIZER_MAX_TIME': 100.0,
                                          "MSK_DPAR_ANA_SOL_INFEAS_TOL": 1e-6,
                                          "MSK_DPAR_BASIS_REL_TOL_S": 1e-6,
                                          "MSK_DPAR_INTPNT_CO_TOL_DFEAS": 1e-5,
                                          "MSK_DPAR_INTPNT_CO_TOL_PFEAS": 1e-5,
                                          "MSK_IPAR_INTPNT_MAX_ITERATIONS": 70,
                                          "MSK_IPAR_NUM_THREADS": 0
                                      })
                        if problem.status == 'optimal':
                            pass
                        else:
                            print(problem.status)
                            print(
                                '------------------------------------------------------------'
                            )
                            if bool(SizeTNoInd_lower_bound) and bool(
                                    SizeTNoInd_upper_bound):
                                constraints_selected = [
                                    True, True, True,
                                    bool(turnover_bound),
                                    bool(industry_deviation),
                                    bool(industry_deviation),
                                    bool(TE),
                                    bool(SizeTNoInd_upper_bound),
                                    bool(SizeTNoInd_lower_bound),
                                    bool(industry_deviation),
                                    bool(industry_deviation),
                                    bool(turnover_bound), False, False, False
                                ]
                                constraints_selected = list(
                                    constraints_all[constraints_selected].
                                    values)
                                problem = cp.Problem(objective,
                                                     constraints_selected)
                                problem.solve(
                                    solver=cp.MOSEK,
                                    mosek_params={
                                        'MSK_DPAR_OPTIMIZER_MAX_TIME': 100.0,
                                        "MSK_DPAR_ANA_SOL_INFEAS_TOL": 1e-12,
                                        "MSK_DPAR_BASIS_REL_TOL_S": 1e-12,
                                        "MSK_DPAR_INTPNT_CO_TOL_DFEAS": 1e-10,
                                        "MSK_DPAR_INTPNT_CO_TOL_PFEAS": 1e-10,
                                        "MSK_IPAR_INTPNT_MAX_ITERATIONS": 70,
                                        "MSK_IPAR_NUM_THREADS": 0
                                    })
                    except Exception as e:
                        pass
                if problem.status == "optimal":
                    if bool(TE):
                        print(a6.value)
        else:
            pass
        if problem.status == 'optimal':
            w_value = self.adjust_weights(w.value)
            return problem.status, w_value, problem
        else:
            return problem.status, w_0, problem

    def bench(self, beg_date, end_date, top_n=10, top_yesterday=90):
        """
        记录等权的状态，需单独运行。
        """
        beg_date = pd.Timestamp(beg_date)
        end_date = pd.Timestamp(end_date)
        self.data = self.data[(self.data['TradingDay'] > beg_date)
                              & (self.data['TradingDay'] < end_date)]
        self.days = self.data['TradingDay'].unique()
        yesterday = None
        secucodes_use = []
        for day in self.days:
            if yesterday is not None:
                secu_use, secucodes_use = self.select_securities_with_R(
                    day,
                    secucodes_use,
                    top_n=top_n,
                    top_yesterday=top_yesterday)
                w_value = np.ones(top_n + top_yesterday).reshape(
                    -1, 1) / (top_n + top_yesterday)
                industry_weight, industry_matrix = self.get_Industry_martix(
                    day=day, secu_use=secu_use, secucodes_use=secucodes_use)
                if True:
                    #追踪工作1：追踪行业权重
                    indus_count_temp = self.ZZ500_weight[
                        self.ZZ500_weight["TradingDay"] ==
                        day].loc[:,
                                 ["FirstIndustryCode", "IndustryWeight"]].copy(
                                 )
                    indus_count_temp["weight"] = industry_matrix.T @ w_value
                    indus_count_temp = indus_count_temp.set_index(
                        "FirstIndustryCode")
                    self.indus_count_bench.append(indus_count_temp)
                    #追踪工作2：追踪当日组合因子大小
                    factor = np.array(secu_use["Factor"]).reshape(
                        -1, 1).T @ w_value
                    self.factor_bench.append(float(factor))
                    #追踪工作3：追踪当日组合size
                    size = secu_use['Size']
                    size = np.diag(size)
                    size = size @ industry_matrix
                    self.size_bentch.append(
                        pd.DataFrame(data=(size.T @ w_value) /
                                     (industry_matrix.T @ w_value + 0.0001),
                                     index=indus_count_temp.index))
                    #追踪工作4：追踪SizeTNoInd
                    #获取当日SizeTNoInd数据
                    SizeTNoInd = secu_use['SizeTNoInd']
                    SizeTNoInd = np.array(SizeTNoInd).reshape(-1, 1)
                    self.SizeTNoInd_bentch.append(float(
                        SizeTNoInd.T @ w_value))

            else:
                secu_all = self.data[self.data['TradingDay'] == day]
                secu_all = secu_all.sort_values(by='expected_return',
                                                ascending=False)
                secu_use = secu_all.head(top_n + top_yesterday)
                w_value = np.ones(top_n + top_yesterday) / (top_n +
                                                            top_yesterday)
                secucodes_use = secu_use['SecuCode']
                temp_s = pd.Series(index=secucodes_use,
                                   data=w_value.reshape(-1))
                temp_s = temp_s.sort_index(ascending=True)
                w_value = temp_s.values
                secucodes_use = temp_s.index
            yesterday = day

    def get_Industry_martix(self, day, secu_use, secucodes_use):
        """
        提取当天的行业的股票权重，与行业矩阵

        Parameters:
        day (pd.Timestamp): 当前处理的日期，今日。
        secu_use (list of int): 当前使用的证券数据，包含"FirstIndustryCode"列。

        Returns:
        tuple: industry_weight与industry_matrix.
        """
        secucodes_number = len(secucodes_use)
        #提取行业权重
        industry_weight = self.ZZ500_weight[self.ZZ500_weight["TradingDay"] ==
                                            day]["IndustryWeight"]
        industry_weight = np.array(industry_weight).reshape(-1, 1)
        #由于直接get_dummies可能存在某个行业在指数内有但是在选取的股票中行业不存在的情况，因此需加一个虚拟的几行需要包括所有的行业。
        all_industries = self.data[self.data["TradingDay"] ==
                                   day]["FirstIndustryCode"].unique().dropna()
        dummy_rows = pd.DataFrame({'FirstIndustryCode': all_industries})
        combined_df = pd.concat([secu_use, dummy_rows], ignore_index=True)

        industry_matrix = pd.get_dummies(
            combined_df["FirstIndustryCode"]).loc[:secucodes_number - 1]

        return industry_weight, np.array(industry_matrix)
    
    def get_SizeGroup_matrix(self,secu_use):
        """
        提取当天的行业的Size_matrix

        Parameters:
        secu_use (list of int): 当前使用的证券数据，包含"Size_group"列。

        Returns:
        tuple: SizeGroup_matrix.
        """
        secucodes_number = secu_use.shape[0]
        #原理与get_industry相同
        dummy_rows = pd.DataFrame({'Size_group': [0,1,2,3,4,5,6,7,8,9]})
        combined_df = pd.concat([secu_use, dummy_rows], ignore_index=True)
        SizeGroup_matrix = pd.get_dummies(combined_df["Size_group"]).iloc[:secucodes_number]
        
        return np.array(SizeGroup_matrix)
    
    def get_securities_number_change(self):
        """
        Returns:
        float:返回整个周期内平均每日换出的股票数量。
        """
        yesterday = None
        number_change = 0
        for day in self.days:
            secu_use_today = self.data[(self.data["TradingDay"] == day)
                                       & (self.data["Weight.new"] != 0)]
            secucodes_use_today = secu_use_today["SecuCode"]
            if yesterday is not None:
                number_change = number_change + len(
                    set(secucodes_use_today) - set(secucodes_use_yesterday))
                secucodes_use_yesterday = secucodes_use_today
                yesterday = day
            else:
                secucodes_use_yesterday = secucodes_use_today
                yesterday = day
        return number_change / (len(self.days) - 1)

    def priority_to_penalty(self, priority_list):
        """
        输入：priority_list一个可迭代对象，用1，2，3代表优先级。
        1最大，2，3次之
        
        Returns:
        float:返回整个周期内平均每日换出的股票数量。
        """
        weight_mapping = {1: 100, 2: 10, 3: 1}

        # 根据优先级列表返回对应的权重
        weights = [weight_mapping[priority] for priority in priority_list]

        return weights

    def get_industry_deviation(self):
        """
        Returns:
        pd.DataFrame:返回整个周期内与行业的平均偏离。
        """
        return reduce(lambda x, y: x.add(y, fill_value=0),
                      self.indus_count) / len(self.indus_count)

    def get_dynamic_weight_bound(self, industry_weight, industry_matrix,
                                 industry_deviation):
        """
        目的：在给定的行业偏离下，在每个行业内部添加权重上下限
        Parameters：
        industry_weight:行业权重
        industry_matrix：行业矩阵（n*29）
        industry_deviation：行业偏离（float）
        Returns:
        pd.DataFrame:返回整个周期内与行业的平均偏离。
        """
        secucodes_number = industry_matrix.shape[0]
        ind_count = industry_matrix.T @ np.ones(secucodes_number).reshape(
            -1, 1)
        industry_weight_with0 = np.where(ind_count == 0, 0, industry_weight)
        ind_upper_weight = (industry_weight_with0 +
                            industry_deviation) / (ind_count + 1e-6)
        ind_upper_weight = np.where(ind_upper_weight > 1, 0, ind_upper_weight)
        ind_upper_weight = np.minimum(ind_upper_weight, 0.02)

        industry_weight_with0 = np.where(ind_count == 0, 0, industry_weight)
        #下限还是按照原本的industry_weight来吧，相比以上少了设置为0的那一步。
        ind_lower_weight = (industry_weight -
                            industry_deviation) / (ind_count + 1e-6)
        ind_lower_weight = np.where(ind_lower_weight < 0, 0.001,
                                    ind_lower_weight)
        ind_lower_weight = np.where(ind_lower_weight > 1, 0, ind_lower_weight)
        ind_lower_weight = np.where(ind_lower_weight > 0.01, 0.01,
                                    ind_lower_weight)
        weight_lower_bound = industry_matrix @ ind_lower_weight
        weight_upper_bound = industry_matrix @ ind_upper_weight

        return weight_lower_bound, weight_upper_bound

    def plot(self):
        """
        Returns:
        6张图表，分别是每日换出票数，行业偏离，组合因子值，换手率,SizeTNOInd的大小，size大小。
        """
        fig = plt.figure(figsize=(10, 10))
        gs = gridspec.GridSpec(4, 2, height_ratios=[2, 2, 2, 2])
        ax1 = fig.add_subplot(gs[0, 0])
        ax1.hist(self.tosell_number_2,
                 bins=30,
                 range=(0, 30),
                 edgecolor='black',
                 alpha=0.7)
        ax1.set_title(
            f'Turnover Number with turnover_bound{self.params["turnover_bound"]}'
        )

        # 第二个小图（行业偏离条形图）
        ax2 = fig.add_subplot(gs[0, 1])
        dev = self.get_industry_deviation()
        dev2 = reduce(lambda x, y: x.add(y, fill_value=0),
                      self.indus_count_bench) / len(self.days)
        x = np.arange(len(dev.index))
        bar_width = 0.35
        ax2.bar(x - bar_width / 2,
                dev["weight"] - dev["IndustryWeight"],
                width=bar_width,
                label='mvo',
                color='orange')
        ax2.bar(x + bar_width / 2,
                dev2["weight"] - dev2["IndustryWeight"],
                width=bar_width,
                label='equal.weight',
                color='b')
        ax2.legend()
        ax2.set_xticks([])
        ax2.set_title(
            f'Industry_Deviation with industry_deviation{self.params["industry_deviation"]}'
        )

        # 第三个大图（组合因子波动）
        ax3 = fig.add_subplot(gs[1, :])
        ax3.plot(self.days[1:],
                 self.factor_bench,
                 color="b",
                 label="equal.weight")
        ax3.plot(self.days[1:], self.factor, color="orange", label="MVO")
        ax3.set_xticks([])
        ax3.legend()
        ax3.set_title(
            f'Factor_Value from{self.params["beg_date"]}to{self.params["end_date"]}'
        )

        # 第四个大图（换手率）
        ax4 = fig.add_subplot(gs[2, :])
        ax4.plot(self.days[1:],
                 np.array(self.tosell_weight_2) * 2,
                 label='to_sell_weight',
                 color='orange')
        ax4.plot(self.days[1:], self.turnover, label='turnover', color='b')
        ax4.set_xticks([])
        ax4.legend()
        ax4.set_title(
            f'Turnover with turnover_bound{self.params["turnover_bound"]}')
        if False:
            #第四个图(size因子)
            ax5 = fig.add_subplot(gs[3, 1])
            dev1 = reduce(lambda x, y: x.add(y, fill_value=0),
                          self.size) / len(self.days)
            dev2 = reduce(lambda x, y: x.add(y, fill_value=0),
                          self.size_bentch) / len(self.days)
            x = np.arange(len(dev.index))
            bar_width = 0.35
            ax5.bar(x - bar_width / 2,
                    dev1.iloc[:, 0],
                    width=bar_width,
                    label='mvo',
                    color='orange')
            ax5.bar(x + bar_width / 2,
                    dev2.iloc[:, 0],
                    width=bar_width,
                    label='equal.weight',
                    color='b')
            ax5.set_xticks([])
            ax5.legend()
            ax5.set_title('size')

        #第6个图，SizeTNoInd
        ax6 = fig.add_subplot(gs[3, :])
        ax6.plot(self.days[1:],
                 self.SizeTNoInd_bentch,
                 label='equal.weight',
                 color='b')
        ax6.plot(self.days[1:], self.SizeTNoInd, label='mvo', color='orange')
        ax6.set_xticks([])
        ax6.legend()
        ax6.set_title(
            f'SizeTNoInd with SizeTNoInd{self.params["SizeTNoInd_lower_bound"]}——{self.params["SizeTNoInd_upper_bound"]}'
        )

        #展现
        plt.tight_layout()
        plt.savefig(str(pd.Timestamp.now().strftime("%Y-%m-%d_%H-%M-%S")) +
                    ".png",
                    transparent=True)
        plt.show()

    def initialize_first_day(self,
                             day,
                             secucodes_number_pool,
                             Mode,
                             dynamic_weight_bound=False):
        """
        初始化第一天优化的权重，由于第一天不存在换手，得单独计算。
        """
        secu_all = self.data[self.data['TradingDay'] == day].sort_values(
            by='expected_return', ascending=False)
        secu_use = secu_all.head(secucodes_number_pool).sort_values(
            by='SecuCode', ascending=True)
        secucodes_use = list(secu_use['SecuCode'])

        if Mode == "A":
            w_value = np.ones(secucodes_number_pool) / secucodes_number_pool

            return secucodes_use, None, w_value, "optimal"
        elif Mode == "B":
            if True:
                secucodes_use = list(secu_use['SecuCode'])
                secu_use = secu_use.sort_values(by='expected_return',
                                                ascending=False)
                secu_use = secu_use.head(100)
                secu_use = secu_use.sort_values(by='SecuCode', ascending=True)
                secucodes_use_2 = list(
                    secu_use["SecuCode"].sort_values(ascending=True).values)
            if False:
                secu_all = self.data[self.data['TradingDay'] == day]
                secu_use = secu_all.groupby("Size_group").apply(
                    lambda x: x.sort_values(
                        by='expected_return', ascending=False).head(
                            int(secucodes_number_pool / 10))).reset_index(
                                drop=True)
                secucodes_use = list(secu_use['SecuCode'])
                secu_use = secu_use.groupby("Size_group").apply(
                    lambda x: x.head(10)).reset_index(drop=True)
                secu_use = secu_use.sort_values(by='SecuCode', ascending=True)
                secucodes_use_2 = list(
                    secu_use["SecuCode"].sort_values(ascending=True).values)
            if dynamic_weight_bound:
                industry_weight, industry_matrix = self.get_Industry_martix(
                    day=day, secu_use=secu_use, secucodes_use=secucodes_use_2)
                indus_equal_value = industry_weight / (
                    industry_matrix.T @ np.ones(100).reshape(-1, 1) + 1e-6)
                indus_equal_value = np.where(indus_equal_value > 1, 0,
                                             indus_equal_value)
                w_value = industry_matrix @ indus_equal_value
                w_value = w_value / np.sum(w_value)
            else:
                w_value = np.ones(100).reshape(-1, 1) / 100
            return secucodes_use, secucodes_use_2, w_value, "optimal"
     
    def calculate_dynamic_weight_upper_bound(self, w_0):
        """
        当Mode是B的时候，
        """
        secucodes_number_pool = w_0.size
        tosell_number_1 = 100 - np.where(w_0 != 0, 1, 0).sum()
        tolerance = (1 - np.sum(w_0)) / (tosell_number_1 * 1.5)
        return np.maximum(
            np.ones(secucodes_number_pool).reshape(-1, 1) * tolerance, w_0)
    
    def save_w2df(self, day, problem_status, w_value, secucodes_use, secucodes_use_2, Mode,w_first_stage=None):
        if problem_status == 'optimal':
            print(str(day)[:10]+"有最优解")
            self.opts += 1
            if Mode == "A":
                self.data.loc[(self.data['TradingDay'] == day) & (self.data['SecuCode'].isin(secucodes_use)), 'OpenRatio'] = w_value
            elif Mode == "B":
                self.data.loc[(self.data['TradingDay'] == day) & (self.data['SecuCode'].isin(secucodes_use)),'First_stage_weight'] = w_first_stage
                self.data.loc[(self.data['TradingDay'] == day) & (self.data['SecuCode'].isin(secucodes_use_2)), 'OpenRatio'] = w_value
        else:
            print(str(day)[:10]+"无最优解")
            self.nonopts += 1
            if Mode == "A":
                self.data.loc[(self.data['TradingDay'] == day) & (self.data['SecuCode'].isin(secucodes_use)), 'OpenRatio'] = 1 / len(secucodes_use)
            elif Mode == "B":
                self.data.loc[(self.data['TradingDay'] == day) & (self.data['SecuCode'].isin(secucodes_use)),'First_stage_weight'] = w_first_stage
                self.data.loc[(self.data['TradingDay'] == day) & (self.data['SecuCode'].isin(secucodes_use_2)), 'OpenRatio'] = 1 / len(secucodes_use_2)
        
    def DrownDown_signal(self,day):
        """
        回撤信号，用于判断是否即将迎来回撤，主要是由头部300的因子与收益的相关性水平来决定的。
        """
        head300 = self.data[self.data["TradingDay"]==day].sort_values(by="Factor",ascending=False).head(300)
        self.factor_300.append(float(np.nanmean(head300["Factor"])))
        
        head100 = self.data[self.data["TradingDay"]==day].sort_values(by="Factor",ascending=False).head(100)
        self.factor_100.append(float(np.nanmean(head100["Factor"])))
        
        Cor_Factor_Return = float(np.correlate(head300["Factor"],head300["Return.H"]))
        self.cors.append(Cor_Factor_Return)
        
        if Cor_Factor_Return<-10:
            return True
        else:
            return False
    
    def select_securities_with_DrownDown(self,
                            day,
                            secucodes_use,
                            top_n=10,
                            top_yesterday=90):
        pass
        
    def opt(self,
            lambda_value,
            beg_date,
            end_date,
            top_n=10,
            top_yesterday=90,
            Mode="A",
            mode=1,
            weight_lower_bound=0,
            weight_upper_bound=0.015,
            industry_deviation=0.1,
            dynamic_weight_bound=False,
            SizeTNoInd_lower_bound=-1,
            SizeTNoInd_upper_bound=1,
            turnover_bound=0.4,
            priority_list=[1, 1, 1],
            TE=0.2):
        """
        优化主体。
        """
        self.data['OpenRatio'] = 0
        self.data["First_stage_weight"] = 0
        beg_date = pd.Timestamp(beg_date)
        end_date = pd.Timestamp(end_date)
        self.data = self.data[(self.data['TradingDay'] > beg_date)
                              & (self.data['TradingDay'] < end_date)]
        self.days = self.data['TradingDay'].unique()
        self.params = {
            "lambda_value": lambda_value,
            "beg_date": beg_date,
            "end_date": end_date,
            "weight_lower_bound": weight_lower_bound,
            "weight_upper_bound": weight_upper_bound,
            "industry_deviation": industry_deviation,
            "dynamic_weight_bound": dynamic_weight_bound,
            "SizeTNoInd_lower_bound": SizeTNoInd_lower_bound,
            "SizeTNoInd_upper_bound": SizeTNoInd_upper_bound,
            "turnover_bound": turnover_bound,
            "TE": TE
        }
        secucodes_number_pool = top_n + top_yesterday
        yesterday = None
        secucodes_use = []
        secucodes_use_2 = []

        for day in self.days:
            if yesterday is not None:
                #获取今日的选股的股票代码以及相关数据。
                secu_use, secucodes_use = self.select_securities_with_R(
                    day,
                    secucodes_use, 
                    top_n=top_n, 
                    top_yesterday=top_yesterday)
                
                #判断今天多少只股票。
                secucodes_number = len(secucodes_use)
                
                #用今日的股票代码提取昨日权重。
                w_0 = self.initialize_weights(secucodes_use=secucodes_use,
                              yesterday=yesterday)
                
                #提取今日使用的股票代码的协方差矩阵与二阶矩。
                sigma = self.sigmas[list(self.data['TradingDay'].unique()).index(day)].copy()
                sec = self.second_moment[list(self.data['TradingDay'].unique()).index(day)].copy()
                
                #提取今日使用的股票代码的期望收益。
                expected_return = np.array(secu_use['expected_return']).reshape(-1, 1)
                
                #根据今日使用的股票代码得到行业矩阵。
                industry_weight, industry_matrix = self.get_Industry_martix(
                    day=day, 
                    secu_use=secu_use, 
                    secucodes_use=secucodes_use)
                
                #获取当日SizeTNoInd数据
                SizeTNoInd = secu_use['SizeTNoInd']
                SizeTNoInd = np.array(SizeTNoInd).reshape(-1, 1)
                
                #获取SizeGroup_matirx
                SizeGroup_matrix = self.get_SizeGroup_matrix(secu_use)
                #数据都有了根据Mode来选择优化方式
                if Mode == "A":
                    #追踪因为股票池变化固定换出的部分
                    self.tosell_weight_1.append(float(1 - sum(w_0)))
                    self.tosell_number_1.append(top_n + top_yesterday -
                                                w_0[w_0 != 0].size)
                    #是否选择动态上下限
                    d_weight_lower_bound, d_weight_upper_bound = self.get_dynamic_weight_bound(
                        industry_matrix=industry_matrix,
                        industry_weight=industry_weight,
                        industry_deviation=industry_deviation
                        ) if dynamic_weight_bound else (weight_lower_bound, weight_upper_bound)
                    #优化
                    problem_status,w_value,solved_problem = self.solve_optimization(
                        mode=1,
                        secucodes_number=secucodes_number,
                        lambda_value=lambda_value,
                        expected_return=expected_return,
                        sigma=sigma,
                        sec=sec,
                        w_0=w_0,
                        industry_weight=industry_weight,
                        industry_matrix=industry_matrix,
                        weight_lower_bound=d_weight_lower_bound,
                        weight_upper_bound=d_weight_upper_bound,
                        industry_deviation=industry_deviation,
                        turnover_bound=turnover_bound,
                        SizeTNoInd=SizeTNoInd,
                        SizeTNoInd_lower_bound=SizeTNoInd_lower_bound,
                        SizeTNoInd_upper_bound=SizeTNoInd_upper_bound,
                        dynamic_weight_bound=dynamic_weight_bound,
                        priority_list=priority_list,
                        SizeGroup_matrix =SizeGroup_matrix,
                        SizeGroup_lower_bound=False,
                        SizeGroup_upper_bound=False,
                        TE=TE)
                elif Mode =="B":
                    #追踪因为股票池变化固定换出的部分
                    self.tosell_weight_1.append(float(1 - sum(w_0)))
                    self.tosell_number_1.append(top_n + top_yesterday -
                                                w_0[w_0 != 0].size)
                    #生成一个动态的上限
                    d_weight_upper_bound = self.calculate_dynamic_weight_upper_bound(w_0 = w_0)
                    #初步优化
                    problem_status, w_value, solved_problem = self.solve_optimization(
                        mode=1,
                        secucodes_number=secucodes_number,
                        lambda_value=lambda_value,
                        expected_return=expected_return,
                        sigma=sigma,
                        sec=sec,
                        w_0=w_0,
                        SizeTNoInd=SizeTNoInd,
                        industry_weight=industry_weight,
                        industry_matrix=industry_matrix,
                        industry_deviation=industry_deviation,
                        SizeTNoInd_lower_bound=False,
                        SizeTNoInd_upper_bound=False,
                        SizeGroup_matrix =SizeGroup_matrix,
                        SizeGroup_lower_bound=False,
                        SizeGroup_upper_bound=False,
                        weight_lower_bound=0,
                        weight_upper_bound=weight_upper_bound
                        if not dynamic_weight_bound else d_weight_upper_bound,
                        dynamic_weight_bound=True,
                        turnover_bound=turnover_bound,
                        priority_list=priority_list,
                        TE=False)
                    #选择最大的100个
                    secu_use, secucodes_use_2 = self.select_securities_with_Weight(
                        day,
                        w_value,
                        secucodes_use=secucodes_use,
                        secucodes_use_2=secucodes_use_2,
                        industry_matrix=industry_matrix)
                    w_first_stage = self.adjust_weights(w_value)
                        
                    #今天选股的股票个数
                    secucodes_number = len(secucodes_use_2)
                    
                    #300选出100后的每日权重
                    w_0 = self.initialize_weights(
                        secucodes_use=secucodes_use_2, yesterday=yesterday)
                    
                    #提取这100个股票的权重
                    sigma = sigma.loc[secucodes_use_2, secucodes_use_2]
                    sec = sec.loc[secucodes_use_2, secucodes_use_2]
                    
                    #获取收益转化为n*1向量
                    expected_return = secu_use['expected_return']
                    expected_return = np.array(expected_return).reshape(
                        secucodes_number, 1)
                    
                    #获取行业权重与行业矩阵
                    industry_weight, industry_matrix = self.get_Industry_martix(
                        day=day,
                        secu_use=secu_use,
                        secucodes_use=secucodes_use_2)
                    
                    #获取权重上下限
                    d_weight_lower_bound, d_weight_upper_bound = self.get_dynamic_weight_bound(
                                            industry_matrix=industry_matrix,
                                            industry_weight=industry_weight,
                                            industry_deviation=industry_deviation
                                            ) if dynamic_weight_bound else (weight_lower_bound, weight_upper_bound)
                    
                    #获取无行业偏离的Size数据
                    SizeTNoInd = secu_use['SizeTNoInd']
                    SizeTNoInd = np.array(SizeTNoInd).reshape(-1, 1)
                    #
                    SizeGroup_matrix = self.get_SizeGroup_matrix(secu_use)
                    #求解
                    problem_status, w_value, solved_problem = self.solve_optimization(
                        mode=1,
                        secucodes_number=secucodes_number,
                        lambda_value=lambda_value,
                        expected_return=expected_return,
                        sigma=sigma,
                        sec=sec,
                        w_0=w_0,
                        SizeTNoInd=SizeTNoInd,
                        SizeTNoInd_lower_bound=SizeTNoInd_lower_bound,
                        SizeTNoInd_upper_bound=SizeTNoInd_upper_bound,
                        industry_weight=industry_weight,
                        industry_matrix=industry_matrix,
                        SizeGroup_matrix =SizeGroup_matrix,
                        SizeGroup_lower_bound=False,
                        SizeGroup_upper_bound=False,
                        industry_deviation=industry_deviation,
                        weight_lower_bound=weight_lower_bound
                        if not dynamic_weight_bound else d_weight_lower_bound,
                        weight_upper_bound=weight_upper_bound
                        if not dynamic_weight_bound else d_weight_upper_bound,
                        turnover_bound=turnover_bound,
                        priority_list=priority_list,
                        dynamic_weight_bound=dynamic_weight_bound,
                        TE=TE)
                
                if True:
                    #追踪
                    w_value = self.adjust_weights(w_value)
                    factor = np.array(secu_use["Factor"]).reshape(-1,
                                                              1).T @ w_value
                    self.Return_H.append(float(w_value.T @ np.array(secu_use["Return.H"]).reshape(-1,1)))
                    #追踪工作1：追踪行业权重
                    self.DrownDown_signal(day)
                    indus_count_temp = self.ZZ500_weight[
                        self.ZZ500_weight["TradingDay"] ==
                        day].loc[:,
                                 ["FirstIndustryCode", "IndustryWeight"]].copy(
                                 )
                    indus_count_temp["weight"] = industry_matrix.T @ w_value
                    indus_count_temp = indus_count_temp.set_index(
                        "FirstIndustryCode")
                    self.indus_count.append(indus_count_temp)
                    #追踪工作2：追踪当日组合因子大小
                    factor = np.array(secu_use["Factor"]).reshape(
                        -1, 1).T @ w_value
                    self.factor.append(float(factor))
                    #追踪工作3：追踪当日组内的换手
                    self.inner_turnover.append(
                        float(np.abs(w_value - w_0).sum() - 1 + sum(w_0)))
                    #追踪工作4：追踪整体换手
                    self.turnover.append(
                        float(np.abs(w_value - w_0).sum()) + 1 - sum(w_0))
                    #追踪工作5：追踪换出的数量
                    self.tosell_weight_2.append(float(1 - sum(w_0)))
                    self.tosell_number_2.append(secucodes_number -
                                                w_0[w_0 != 0].size)
                    #追踪工作7：追踪SizeTNoInd
                    self.SizeTNoInd.append(float(SizeTNoInd.T @ w_value))
                    #追踪工作8：追踪每天的股票数量
                    self.indus_number.append(
                        industry_matrix.T @ np.ones(secucodes_number).reshape(
                            -1, 1)) 
                else:
                    pass
            else:
                secucodes_use, secucodes_use_2, w_value, problem_status = self.initialize_first_day(
                    day,
                    secucodes_number_pool,
                    Mode=Mode,
                    dynamic_weight_bound=dynamic_weight_bound)
                w_first_stage =None
            
            self.save_w2df(day, problem_status,w_value,secucodes_use, secucodes_use_2, Mode,w_first_stage)
            yesterday = day
