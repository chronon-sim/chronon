// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Type-erased port handles and global registry for path-based port discovery.
// Enables YAML-driven port connection by resolving string paths to actual ports.
//
// Uses forward declarations only to avoid circular dependencies. Port.hpp
// includes this header and provides the automatic registration.

#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>

#include "../core/Fwd.hpp"

namespace chronon::sender {

class Unit;
class TickSimulation;
class PortBase;
template <typename T>
class InPort;
template <typename T>
class OutPort;

/**
 * IPortHandle - Type-erased base class for port handles.
 *
 * Enables runtime port discovery and type-safe binding without
 * knowing concrete types at compile time.
 */
class IPortHandle {
public:
    virtual ~IPortHandle() = default;

    virtual const std::type_info& dataType() const = 0;
    virtual std::type_index dataTypeIndex() const = 0;
    virtual bool isInPort() const = 0;
    virtual bool isOutPort() const = 0;
    virtual const std::string& name() const = 0;
    virtual const std::string& fullPath() const = 0;
    virtual Unit* owner() const = 0;
    virtual PortBase* portBase() const = 0;

    /**
     * Connect this port to another port (for OutPort -> InPort binding).
     *
     * @param other The destination port handle
     * @param delay Connection delay in cycles
     * @return Pointer to the created ConnectionBase, or nullptr on failure
     */
    virtual ConnectionBase* connectTo(IPortHandle* other, uint32_t delay) = 0;
};

/// Typed handle for input ports.
template <typename T>
class InPortHandle : public IPortHandle {
public:
    InPortHandle(InPort<T>* port, Unit* owner, std::string name, std::string full_path)
        : port_(port), owner_(owner), name_(std::move(name)), full_path_(std::move(full_path)) {}

    const std::type_info& dataType() const override { return typeid(T); }
    std::type_index dataTypeIndex() const override { return std::type_index(typeid(T)); }
    bool isInPort() const override { return true; }
    bool isOutPort() const override { return false; }
    const std::string& name() const override { return name_; }
    const std::string& fullPath() const override { return full_path_; }
    Unit* owner() const override { return owner_; }
    PortBase* portBase() const override;  ///< Defined after PortBase is complete

    /// InPort cannot initiate connections.
    ConnectionBase* connectTo(IPortHandle* /*other*/, uint32_t /*delay*/) override {
        return nullptr;
    }

    InPort<T>* port() const { return port_; }

private:
    InPort<T>* port_;
    Unit* owner_;
    std::string name_;
    std::string full_path_;
};

/// Typed handle for output ports.
template <typename T>
class OutPortHandle : public IPortHandle {
public:
    OutPortHandle(OutPort<T>* port, Unit* owner, std::string name, std::string full_path)
        : port_(port), owner_(owner), name_(std::move(name)), full_path_(std::move(full_path)) {}

    const std::type_info& dataType() const override { return typeid(T); }
    std::type_index dataTypeIndex() const override { return std::type_index(typeid(T)); }
    bool isInPort() const override { return false; }
    bool isOutPort() const override { return true; }
    const std::string& name() const override { return name_; }
    const std::string& fullPath() const override { return full_path_; }
    Unit* owner() const override { return owner_; }
    PortBase* portBase() const override;  ///< Defined after PortBase is complete

    /// Connect this output port to an input port handle. Uses
    /// OutPort::connect() directly without requiring Simulation.
    /// Defined in OutPort.hpp.
    ConnectionBase* connectTo(IPortHandle* other, uint32_t delay) override;

    OutPort<T>* port() const { return port_; }

private:
    OutPort<T>* port_;
    Unit* owner_;
    std::string name_;
    std::string full_path_;
};

/**
 * PortDirectory - Singleton registry for port discovery.
 *
 * Thread-safe registration and lookup by full path.
 */
class PortDirectory {
public:
    static PortDirectory& instance() {
        static PortDirectory dir;
        return dir;
    }

    void registerPort(const std::string& full_path, std::unique_ptr<IPortHandle> handle) {
        std::lock_guard<std::mutex> lock(mutex_);
        ports_[full_path] = std::move(handle);
    }

    IPortHandle* findPort(const std::string& full_path) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = ports_.find(full_path);
        return it != ports_.end() ? it->second.get() : nullptr;
    }

    bool hasPort(const std::string& full_path) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return ports_.count(full_path) > 0;
    }

    void clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        ports_.clear();
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return ports_.size();
    }

private:
    PortDirectory() = default;
    PortDirectory(const PortDirectory&) = delete;
    PortDirectory& operator=(const PortDirectory&) = delete;

    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<IPortHandle>> ports_;
};

/**
 * PortBindingRegistry - Provides type-safe port binding.
 *
 * Uses virtual dispatch via IPortHandle::connectTo() for type-safe binding
 * without requiring Simulation in the header dependency chain.
 */
class PortBindingRegistry {
public:
    static PortBindingRegistry& instance() {
        static PortBindingRegistry reg;
        return reg;
    }

    /**
     * Bind an output port to an input port.
     *
     * Uses virtual dispatch to handle type-safe connection without
     * requiring TickSimulation in the include chain.
     *
     * @param out_handle Output port handle
     * @param in_handle Input port handle
     * @param delay Connection delay in cycles
     * @return Pointer to the created ConnectionBase for dependency registration
     */
    ConnectionBase* bind(IPortHandle* out_handle, IPortHandle* in_handle, uint32_t delay) {
        if (!out_handle->isOutPort()) {
            throw std::runtime_error("First port must be an OutPort: " + out_handle->fullPath());
        }
        if (!in_handle->isInPort()) {
            throw std::runtime_error("Second port must be an InPort: " + in_handle->fullPath());
        }
        if (out_handle->dataTypeIndex() != in_handle->dataTypeIndex()) {
            throw std::runtime_error(
                "Type mismatch: " + out_handle->fullPath() + " (" + out_handle->dataType().name() +
                ") -> " + in_handle->fullPath() + " (" + in_handle->dataType().name() + ")");
        }

        auto* conn = out_handle->connectTo(in_handle, delay);
        if (!conn) {
            throw std::runtime_error("Failed to connect: " + out_handle->fullPath() + " -> " +
                                     in_handle->fullPath());
        }

        return conn;
    }

private:
    PortBindingRegistry() = default;
    PortBindingRegistry(const PortBindingRegistry&) = delete;
    PortBindingRegistry& operator=(const PortBindingRegistry&) = delete;
};

}  // namespace chronon::sender
