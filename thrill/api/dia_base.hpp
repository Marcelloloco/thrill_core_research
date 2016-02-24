/*******************************************************************************
 * thrill/api/dia_base.hpp
 *
 * Untyped super class of DIANode. Used to build the execution graph.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_DIA_BASE_HEADER
#define THRILL_API_DIA_BASE_HEADER

#include <thrill/api/context.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * Possible states a DIABase can be in.
 */
enum class DIAState {
    //! The DIABase has not been computed yet.
    NEW,
    //! The DIABase has been calculated but not explicitly cached.  Data might
    //! be available or has to be recalculated when needed
    EXECUTED,
    //! The DIABase is manually disposed by the user, needs to be recomputed
    //! when accessed.
    DISPOSED
};

/*!
 * Description of the amount of RAM the internal data structures of a DIANode
 * require. Each DIANode implementation can specify this for its PreOp, Execute,
 * and PostOp parts individually. The StageBuilder collects all requests,
 * notifys the BlockPool to reduce its memory limits, and delivers the available
 * amount to the DIANode in StartPreOp(), Execute(), and PushData() calls.
 */
class DIAMemUse
{
public:
    //! Implicit conversion of a size_t for a constant RAM usage request
    DIAMemUse(size_t limit = 0) // NOLINT: implicit conversions desired
        : limit_(limit) { }

    //! Maximum available RAM requested (limit will be determined in
    //! StageBuilder by detecting the DIANodes in a Stage)
    static DIAMemUse Max() { return DIAMemUse(max_limit_); }

    //! return amount of RAM reserved
    size_t limit() const { return limit_; }

    //! test if sentinel for maximum RAM request
    bool is_max() const { return limit_ == max_limit_; }

    //! implicit conversion to size_, but only if not is_max()
    operator size_t () const { assert(!is_max()); return limit_; }

private:
    //! amount of RAM requested or reserved.
    size_t limit_;

    //! sentinel for maximum available RAM.
    static const size_t max_limit_ = static_cast<size_t>(-1);
};

/*!
 * The DIABase is the untyped super class of DIANode. DIABases are used to build
 * the execution graph, which is used to execute the computation.
 *
 * Each DIABase knows it's parents. Parents are node which have to computed
 * previously. Not all DIABases have children (ActionNodes do not), hence,
 * children are first introduced in DIANode.
 */
class DIABase : public std::enable_shared_from_this<DIABase>
{
public:
    using DIABasePtr = std::shared_ptr<DIABase>;

    /*!
     * The constructor for a DIABase. Sets the parents for this node, but does
     * not register it has a child, since this must be done with a callback.
     */
    DIABase(Context& ctx, const char* label,
            const std::initializer_list<size_t>& parent_ids,
            const std::initializer_list<DIABasePtr>& parents)
        : context_(ctx), id_(ctx.next_dia_id()),
          label_(label), parents_(parents) {
        logger_ << "class" << "DIABase"
                << "event" << "create"
                << "type" << "DOp"
                << "label" << label
                << "parents" << parent_ids;
    }

    //! non-copyable: delete copy-constructor
    DIABase(const DIABase&) = delete;
    //! non-copyable: delete assignment operator
    DIABase& operator = (const DIABase&) = delete;
    //! move-constructor: default
    DIABase(DIABase&&) = default;
    //! move-assignment operator: default
    DIABase& operator = (DIABase&&) = default;

    //! Virtual destructor for a DIABase.
    virtual ~DIABase() {
        // Remove child pointer from parent If a parent loses all its childs its
        // reference count should be zero and he should be removed

        logger_ << "class" << "DIABase"
                << "event" << "destroy"
                << "parents" << parent_ids();

        // de-register at parents (if still hooked there)
        for (const DIABasePtr& p : parents_)
            p->RemoveChild(this);
    }

    //! Virtual method to determine whether a node can be Executed() = run such
    //! that it self-contains its data. This is currently true for all Nodes
    //! except Collapse().
    virtual bool CanExecute() { return true; }

    //! \name Pure Virtual Methods called by StageBuilder
    //! {

    //! Amount of RAM used by PreOp after StartPreOp()
    virtual DIAMemUse PreOpMemUse() { return 0; }

    //! Virtual method for preparing start of PushData.
    virtual void StartPreOp(size_t /* id */) { }

    //! Virtual method for preparing end of PushData.
    virtual void StopPreOp(size_t /* id */) { }

    //! Virtual execution method. Triggers actual computation in sub-classes.
    virtual void Execute() = 0;

    //! Amount of RAM used by PushData()
    virtual DIAMemUse PushDataMemUse() { return 0; }

    //! Virtual method for pushing data. Triggers actual pushing in sub-classes.
    virtual void PushData(bool consume) = 0;

    //! Virtual clear method. Triggers actual disposing in sub-classes.
    virtual void Dispose() { }

    //! }

    //! Performing push operation. Notifies children and calls actual push
    //! method. Then cleans up the DIA graph by freeing parent references of
    //! children.
    virtual void RunPushData() = 0;

    //! Returns the children of this DIABase.
    virtual std::vector<DIABase*> children() const = 0;

    //! Virtual method for removing a child.
    virtual void RemoveChild(DIABase* node) = 0;

    //! Virtual method for removing all childs. Triggers actual removing in
    //! sub-classes.
    virtual void RemoveAllChildren() = 0;

    //! Returns the api::Context of this DIABase.
    Context & context() {
        return context_;
    }

    //! return unique id() of DIANode subclass as stored by StatsNode
    const size_t & id() const {
        return id_;
    }

    //! return label() of DIANode subclass as stored by StatsNode
    const char * label() const {
        return label_;
    }

    //! make ostream-able.
    friend std::ostream& operator << (std::ostream& os, const DIABase& d);

    //! Returns consume_counter_
    size_t consume_counter() const { return consume_counter_; }

    //! Virtual SetConsume flag which is called by the user via .Keep() or
    //! .Consume() to set consumption.
    virtual void IncConsumeCounter(size_t counter) {
        consume_counter_ += counter;
    }

    //! Returns the parents of this DIABase.
    const std::vector<DIABasePtr> & parents() const {
        return parents_;
    }

    //! Returns the parents of this DIABase.
    std::vector<size_t> parent_ids() const {
        std::vector<size_t> ids;
        for (const DIABasePtr& p : parents_) ids.push_back(p->id());
        return ids;
    }

    //! Remove a parent
    void RemoveParent(DIABase* p) {
        parents_.erase(
            std::remove_if(
                parents_.begin(), parents_.end(),
                [p](const DIABasePtr& parent) { return parent.get() == p; }),
            parents_.end());
    }

    //! Run Scope and parents such that this node (usually an ActionNode) is
    //! EXECUTED.
    void RunScope();

    //! Return the Context's memory manager
    mem::Manager & mem_manager() {
        return context_.mem_manager();
    }

    DIAState state() const { return state_; }

    void set_state(const DIAState& state) { state_ = state; }

    void set_mem_limit(const DIAMemUse& mem_limit) { mem_limit_ = mem_limit; }

protected:
    //! \name Fixed DIA Information
    //! {

    //! associated Context
    Context& context_;

    //! DIA serial id
    const size_t id_;

    //! DOp node static label.
    const char* const label_;

    //! }

    //! \name Runtime Operational Variables
    //! {

    //! State of the DIANode. State is NEW on creation.
    DIAState state_ = DIAState::NEW;

    //! Parents of this DIABase.
    std::vector<DIABasePtr> parents_;

    //! Amount of memory the current execution stage of the DIA implementation
    //! is allowed to use.
    DIAMemUse mem_limit_ = 0;

    //! Consumption counter: when it reaches zero, PushData() is called with
    //! consume = true
    size_t consume_counter_ = 1;

    //! Never full consume
    static const size_t never_consume_ = static_cast<size_t>(-1);

    //! }

public:
    /**************************************************************************/
    // JsonLogger for this DIANode

    common::JsonLogger logger_ {
        &context_.logger_, "node_id", id(), "node_label", label()
    };
};

using DIABasePtr = std::shared_ptr<DIABase>;

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_DIA_BASE_HEADER

/******************************************************************************/
