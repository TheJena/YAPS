operations = {
    "Split dataset into training set and test set": (
        "Split the dataset into a training set and a test set using train_test_split from scikit-learn",
        """X_train, X_test, y_train, y_test = train_test_split(
            X, Y, test_size=0.3, random_state=1
        )""",
    ),
    "Train a DecisionTreeClassifier": (
        "Train a DecisionTreeClassifier using the training data and evaluate its accuracy",
        """clf = DecisionTreeClassifier()
        clf = clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)""",
    ),
    "Create Decision Tree classifer object with Data Pruning": (
        "Create a DecisionTreeClassifier object with entropy criterion and max depth 3, train it using the training data and evaluate its accuracy",
        """clf = DecisionTreeClassifier(criterion="entropy", max_depth=3)
        clf = clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)""",
    ),
}
