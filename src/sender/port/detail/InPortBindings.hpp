// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#pragma once

// Included after InPort is complete: cold type-erased connection bindings.
namespace chronon::sender {

template <typename T>
IMultiProducerPort* Connection<T>::registerOnDestMPSC() {
    if (thread_queue_id_ == SIZE_MAX || !to_) {
        return nullptr;
    }
    to_->registerMPSCConnection(this);
    return static_cast<IMultiProducerPort*>(to_);
}

template <typename T>
bool Connection<T>::finalizeTransparentBroadcastForDestination(size_t producer_count) {
    return to_ && to_->finalizeTransparentBroadcastReplay(producer_count);
}

template <typename T>
PortBase* InPortHandle<T>::portBase() const {
    return port_;
}

}  // namespace chronon::sender
