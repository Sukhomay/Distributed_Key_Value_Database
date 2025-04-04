#include <iostream>
#include <vector>
#include <string>
#include <algorithm>
#include <bits/stdc++.h>

using namespace std;

struct AVLTreeNode
{
    AVLTreeNode *left;
    AVLTreeNode *right;

    string key;
    string value;
    int count;
    int height;

    AVLTreeNode(const string &key, const string &value);
    void updateValues();
    int balanceFactor();

    AVLTreeNode *left_rotate();
    AVLTreeNode *right_rotate();
};

AVLTreeNode::AVLTreeNode(const string &key, const string &value)
    : key(key), value(value), left(nullptr), right(nullptr), count(1), height(1) {}

void AVLTreeNode::updateValues()
{
    count = (left ? left->count : 0) + (right ? right->count : 0) + 1;
    height = max(left ? left->height : 0, right ? right->height : 0) + 1;
}

int AVLTreeNode::balanceFactor()
{
    return (left ? left->height : 0) - (right ? right->height : 0);
}

AVLTreeNode *AVLTreeNode::left_rotate()
{
    AVLTreeNode *R = right;
    right = right->left;
    R->left = this;
    updateValues();
    R->updateValues();
    return R;
}

AVLTreeNode *AVLTreeNode::right_rotate()
{
    AVLTreeNode *L = left;
    left = left->right;
    L->right = this;
    updateValues();
    L->updateValues();
    return L;
}

class AVLTree
{
    AVLTreeNode *root;
    int _size;

    void balance(vector<AVLTreeNode **> &path);
    void clearHelper(AVLTreeNode *node);

public:
    AVLTree();
    ~AVLTree();

    void insert(const string &key, const string &value);
    void erase(const string &key);
    void clear();
    bool empty() const;
    int size() const;
    vector<pair<string, string>> getSortedPairs() const;

    pair<bool, string> find(const string &key) const;
    string operator[](int idx) const;
};

AVLTree::AVLTree() : root(nullptr), _size(0) {}

AVLTree::~AVLTree()
{
    clear();
}

void AVLTree::clear()
{
    clearHelper(root);
    root = nullptr;
    _size = 0;
}

void AVLTree::clearHelper(AVLTreeNode *node)
{
    if (node)
    {
        clearHelper(node->left);
        clearHelper(node->right);
        delete node;
    }
}

bool AVLTree::empty() const
{
    return _size == 0;
}

int AVLTree::size() const
{
    return _size;
}

void AVLTree::insert(const string &key, const string &value)
{   
    AVLTreeNode **indirect = &root;
    vector<AVLTreeNode **> path;

    while (*indirect != nullptr)
    {
        path.push_back(indirect);

        if ((*indirect)->key == key)
        {
            (*indirect)->value = value;
            return;
        }
        else if ((*indirect)->key > key)
            indirect = &((*indirect)->left);
        else
            indirect = &((*indirect)->right);
    }

    *indirect = new AVLTreeNode(key, value);
    path.push_back(indirect);

    balance(path);
    _size++;
}

void AVLTree::erase(const string &key)
{
    AVLTreeNode **indirect = &root;
    vector<AVLTreeNode **> path;

    while (*indirect != nullptr && (*indirect)->key != key)
    {
        path.push_back(indirect);

        if ((*indirect)->key > key)
            indirect = &((*indirect)->left);
        else
            indirect = &((*indirect)->right);
    }

    if (*indirect == nullptr)
        return;

    AVLTreeNode *toRemove = *indirect;
    if (!(*indirect)->left && !(*indirect)->right)
    {
        *indirect = nullptr;
    }
    else if (!(*indirect)->right)
    {
        *indirect = (*indirect)->left;
    }
    else if (!(*indirect)->left)
    {
        *indirect = (*indirect)->right;
    }
    else
    {
        AVLTreeNode **successor = &((*indirect)->right);
        while ((*successor)->left)
        {
            path.push_back(successor);
            successor = &((*successor)->left);
        }

        AVLTreeNode *temp = *successor;
        *successor = (*successor)->right;

        temp->left = (*indirect)->left;
        temp->right = (*indirect)->right;
        *indirect = temp;
    }

    delete toRemove;
    balance(path);
    _size--;
}

pair<bool, string> AVLTree::find(const string &key) const
{
    AVLTreeNode *direct = root;

    while (direct != nullptr)
    {
        if (direct->key == key)
            return make_pair(true, direct->value);
        else if (direct->key > key)
            direct = direct->left;
        else
            direct = direct->right;
    }
    return make_pair(false, "tombstone"); // Key not found
}

string AVLTree::operator[](int idx) const
{
    AVLTreeNode *cur = root;
    int left = cur->left ? cur->left->count : 0;

    while (left != idx)
    {
        if (left < idx)
        {
            idx -= left + 1;
            cur = cur->right;
            left = cur->left ? cur->left->count : 0;
        }
        else
        {
            cur = cur->left;
            left = cur->left ? cur->left->count : 0;
        }
    }

    return cur->value;
}

void AVLTree::balance(vector<AVLTreeNode **> &path)
{
    reverse(path.begin(), path.end());

    for (auto indirect : path)
    {
        (*indirect)->updateValues();

        if ((*indirect)->balanceFactor() >= 2 && (*indirect)->left->balanceFactor() >= 1)
            *indirect = (*indirect)->right_rotate();

        else if ((*indirect)->balanceFactor() >= 2)
        {
            (*indirect)->left = (*indirect)->left->left_rotate();
            *indirect = (*indirect)->right_rotate();
        }
        else if ((*indirect)->balanceFactor() <= -2 && (*indirect)->right->balanceFactor() <= -1)
            *indirect = (*indirect)->left_rotate();

        else if ((*indirect)->balanceFactor() <= -2)
        {
            (*indirect)->right = (*indirect)->right->right_rotate();
            *indirect = (*indirect)->left_rotate();
        }
    }
}

vector<pair<string, string>> AVLTree::getSortedPairs() const
{
    vector<pair<string, string>> result;
    function<void(AVLTreeNode *)> inOrderTraversal = [&](AVLTreeNode *node)
    {
        if (node == nullptr)
            return;
        inOrderTraversal(node->left);
        result.emplace_back(node->key, node->value);
        inOrderTraversal(node->right);
    };
    inOrderTraversal(root);
    return result;
}

// int main()
// {
//     AVLTree tree;

//     // Insertion of unique (key, value) pairs
//     tree.insert("apple", "A fruit");
//     tree.insert("banana", "A yellow fruit");
//     tree.insert("cherry", "A red fruit");

//     // Display elements by key access
//     cout << "Finding 'apple': " << tree.find("apple") << endl;
//     tree.insert("apple", "A fruitafsgh");
//     cout << "Finding 'apple': " << tree.find("apple") << endl;

//     // Erase a key
//     tree.erase("banana");

//     // Check remaining size and values
//     cout << "Size of tree: " << tree.size() << endl;

//     // Try finding a deleted key
//     cout << "Finding 'banana': " << tree.find("banana") << endl;

//     // Access element by index
//     cout << "Element at index 0: " << tree[1] << endl;
//     vector<pair<string, string>> v = tree.getSortedPairs();
//     for (auto &&i : v)
//     {
//         cout << i.first << " " << i.second << endl;
//     }
//     return 0;
// }
