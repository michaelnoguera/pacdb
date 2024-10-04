"""Budget Accountant Class"""
class BudgetAccountant:

    def __init__(self, max_mi=1/.8) -> None:
        self.privacy_budget = max_mi


    # TODO: Implement methods to track MI consumption - 
    # I presume this will be needed for the cases in which we consume the budget more than once