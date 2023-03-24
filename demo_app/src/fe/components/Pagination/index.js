import React from 'react';
import ReactPaginate from 'react-paginate';

const Pagination = ({numItems, onPageChange, itemsPerPage}) => {
  const pageCount = Math.ceil(numItems / itemsPerPage);

  return (
    <ReactPaginate
      pageCount={pageCount}
      nextLabel="next >"
      pageRangeDisplayed={3}
      marginPagesDisplayed={3}
      previousLabel="< previous"
      pageClassName="page-item"
      pageLinkClassName="page-link"
      previousClassName="page-item"
      previousLinkClassName="page-link"
      nextClassName="page-item"
      nextLinkClassName="page-link"
      breakLabel="..."
      breakClassName="page-item"
      breakLinkClassName="page-link"
      containerClassName="pagination"
      activeClassName="active"
      renderOnZeroPageCount={null}
      onPageChange={onPageChange}
    />
  );
};

export default Pagination;
