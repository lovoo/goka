// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/lovoo/goka/storage (interfaces: Storage)
//
// Generated by this command:
//
//	mockgen -self_package github.com/lovoo/goka -package goka -destination mockstorage.go github.com/lovoo/goka/storage Storage
//

// Package goka is a generated GoMock package.
package goka

import (
	reflect "reflect"

	storage "github.com/lovoo/goka/storage"
	gomock "go.uber.org/mock/gomock"
)

// MockStorage is a mock of Storage interface.
type MockStorage struct {
	ctrl     *gomock.Controller
	recorder *MockStorageMockRecorder
	isgomock struct{}
}

// MockStorageMockRecorder is the mock recorder for MockStorage.
type MockStorageMockRecorder struct {
	mock *MockStorage
}

// NewMockStorage creates a new mock instance.
func NewMockStorage(ctrl *gomock.Controller) *MockStorage {
	mock := &MockStorage{ctrl: ctrl}
	mock.recorder = &MockStorageMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockStorage) EXPECT() *MockStorageMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockStorage) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockStorageMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockStorage)(nil).Close))
}

// Delete mocks base method.
func (m *MockStorage) Delete(key string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete", key)
	ret0, _ := ret[0].(error)
	return ret0
}

// Delete indicates an expected call of Delete.
func (mr *MockStorageMockRecorder) Delete(key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockStorage)(nil).Delete), key)
}

// Get mocks base method.
func (m *MockStorage) Get(key string) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Get", key)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Get indicates an expected call of Get.
func (mr *MockStorageMockRecorder) Get(key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Get", reflect.TypeOf((*MockStorage)(nil).Get), key)
}

// GetOffset mocks base method.
func (m *MockStorage) GetOffset(def int64) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetOffset", def)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetOffset indicates an expected call of GetOffset.
func (mr *MockStorageMockRecorder) GetOffset(def any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOffset", reflect.TypeOf((*MockStorage)(nil).GetOffset), def)
}

// Has mocks base method.
func (m *MockStorage) Has(key string) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Has", key)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Has indicates an expected call of Has.
func (mr *MockStorageMockRecorder) Has(key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Has", reflect.TypeOf((*MockStorage)(nil).Has), key)
}

// Iterator mocks base method.
func (m *MockStorage) Iterator() (storage.Iterator, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Iterator")
	ret0, _ := ret[0].(storage.Iterator)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Iterator indicates an expected call of Iterator.
func (mr *MockStorageMockRecorder) Iterator() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Iterator", reflect.TypeOf((*MockStorage)(nil).Iterator))
}

// IteratorWithRange mocks base method.
func (m *MockStorage) IteratorWithRange(start, limit []byte) (storage.Iterator, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IteratorWithRange", start, limit)
	ret0, _ := ret[0].(storage.Iterator)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// IteratorWithRange indicates an expected call of IteratorWithRange.
func (mr *MockStorageMockRecorder) IteratorWithRange(start, limit any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IteratorWithRange", reflect.TypeOf((*MockStorage)(nil).IteratorWithRange), start, limit)
}

// MarkRecovered mocks base method.
func (m *MockStorage) MarkRecovered() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MarkRecovered")
	ret0, _ := ret[0].(error)
	return ret0
}

// MarkRecovered indicates an expected call of MarkRecovered.
func (mr *MockStorageMockRecorder) MarkRecovered() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MarkRecovered", reflect.TypeOf((*MockStorage)(nil).MarkRecovered))
}

// Open mocks base method.
func (m *MockStorage) Open() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Open")
	ret0, _ := ret[0].(error)
	return ret0
}

// Open indicates an expected call of Open.
func (mr *MockStorageMockRecorder) Open() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Open", reflect.TypeOf((*MockStorage)(nil).Open))
}

// Set mocks base method.
func (m *MockStorage) Set(key string, value []byte) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Set", key, value)
	ret0, _ := ret[0].(error)
	return ret0
}

// Set indicates an expected call of Set.
func (mr *MockStorageMockRecorder) Set(key, value any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Set", reflect.TypeOf((*MockStorage)(nil).Set), key, value)
}

// SetOffset mocks base method.
func (m *MockStorage) SetOffset(offset int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetOffset", offset)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetOffset indicates an expected call of SetOffset.
func (mr *MockStorageMockRecorder) SetOffset(offset any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetOffset", reflect.TypeOf((*MockStorage)(nil).SetOffset), offset)
}
